/*
 *
 *  *
 *  * Copyright (c) 2024 Govcraft.
 *  *
 *  *  Licensed under the Business Source License, Version 1.1 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *      https://github.com/GovCraft/akton-framework/tree/main/LICENSES
 *  *
 *  *  Change Date: Three years from the release date of this version of the Licensed Work.
 *  *  Change License: Apache License, Version 2.0
 *  *
 *  *  Usage Limitations:
 *  *    - You may use the Licensed Work for non-production purposes only, such as internal testing, development, and experimentation.
 *  *    - You may not use the Licensed Work for any production or commercial purpose, including, but not limited to, the provision of any service to third parties, without a commercial use license from the Licensor, except as stated in the Exemptions section of the License.
 *  *
 *  *  Exemptions:
 *  *    - Open Source Projects licensed under an OSI-approved open source license.
 *  *    - Non-Profit Organizations using the Licensed Work for non-commercial purposes.
 *  *    - Small For-Profit Companies with annual gross revenues not exceeding $2,000,000 USD.
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */

use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use akton_arn::Arn;
use dashmap::DashMap;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::time::timeout;
use tokio_util::task::TaskTracker;
use tracing::*;

use crate::common::{ActorRef, Akton, AktonInner, BrokerContext, ParentContext, ReactorItem, ReactorMap, StopSignal, SystemSignal};
use crate::message::{BrokerRequestEnvelope, Envelope, OutboundEnvelope};
use crate::pool::{PoolBuilder, PoolItem};
use crate::prelude::AktonReady;
use crate::traits::Actor;

use super::{ActorConfig, Awake, Idle};

pub struct ManagedActor<RefType: Send + 'static, ManagedEntity: Default + Send + Debug + 'static> {
    pub setup: RefType,

    pub actor_ref: ActorRef,

    pub parent: Option<ParentContext>,

    pub broker: BrokerContext,

    pub halt_signal: StopSignal,

    pub key: String,
    pub akton: AktonReady,

    pub entity: ManagedEntity,

    pub(crate) tracker: TaskTracker,

    pub inbox: Receiver<Envelope>,
    pub(crate) pool_supervisor: DashMap<String, PoolItem>,
}

impl<ManagedEntity: Default + Send + Debug + 'static> Default for ManagedActor<Idle<ManagedEntity>, ManagedEntity> {
    fn default() -> Self {
        let (outbox, inbox) = channel(255);
        let mut actor_ref: ActorRef = Default::default();
        actor_ref.outbox = Some(outbox.clone());

        ManagedActor {
            inbox,
            actor_ref,
            ..Default::default()
        }
    }
}

/// Custom implementation of the `Debug` trait for the `Actor` struct.
///
/// This implementation provides a formatted output for the `Actor` struct, primarily focusing on the `key` field.
impl<RefType: Send + 'static, State: Default + Send + Debug + 'static> Debug
for ManagedActor<RefType, State>
{
    /// Formats the `Actor` struct using the given formatter.
    ///
    /// # Parameters
    /// - `f`: The formatter used for writing formatted output.
    ///
    /// # Returns
    /// A result indicating whether the formatting was successful.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Actor")
            .field("key", &self.key) // Only the key field is included in the debug output
            .finish()
    }
}

/// Represents an actor in the awake state.
///
/// # Type Parameters
/// - `State`: The type representing the state of the actor.
impl<State: Default + Send + Debug + 'static> ManagedActor<Awake<State>, State> {
    /// Creates a new outbound envelope for the actor.
    ///
    /// # Returns
    /// An optional `OutboundEnvelope` if the context's outbox is available.
    pub fn new_envelope(&self) -> Option<OutboundEnvelope> {
        if let Some(envelope) = &self.actor_ref.outbox {
            Option::from(OutboundEnvelope::new(
                Some(envelope.clone()),
                self.key.clone(),
            ))
        } else {
            None
        }
    }

    /// Creates a new parent envelope for the actor.
    ///
    /// # Returns
    /// A clone of the parent's return envelope.
    pub fn new_parent_envelope(&self) -> Option<OutboundEnvelope> {
        if let Some(parent) = &self.parent {
            Some(parent.return_address().clone())
        } else {
            None
        }
    }

    #[instrument(skip(reactors, self))]
    pub(crate) async fn wake(&mut self, reactors: ReactorMap<State>) {
        (self.setup.on_wake)(self);

        while let Some(mut incoming_envelope) = self.inbox.recv().await {
            let type_id;
            let mut envelope;

            // Special case for BrokerRequestEnvelope
            if let Some(broker_request_envelope) = incoming_envelope.message.as_any().downcast_ref::<BrokerRequestEnvelope>() {
                envelope = Envelope::new(
                    broker_request_envelope.message.clone(),
                    incoming_envelope.return_address.clone(),
                    incoming_envelope.pool_id.clone(),
                );
                type_id = broker_request_envelope.message.as_any().type_id().clone();
            } else {
                envelope = incoming_envelope;
                type_id = envelope.message.as_any().type_id().clone();
            }

            if let Some(ref pool_id) = &envelope.pool_id {
                if let Some(mut pool_def) = self.pool_supervisor.get_mut(pool_id) {
                    let pool_clone = pool_def.pool.clone();
                    if let Some(index) = pool_def.strategy.select_context(&pool_clone) {
                        let context = &pool_def.pool[index];
                        trace!(pool_item=context.key,index = index, "Emitting to pool item");
                        context.emit(envelope.message, None).await;
                    }
                }
            } else if let Some(reactor) = reactors.get(&type_id) {
                match reactor.value() {
                    ReactorItem::Message(reactor) => (*reactor)(self, &mut envelope),
                    ReactorItem::Future(fut) => fut(self, &mut envelope).await,
                    _ => tracing::warn!("Unknown ReactorItem type for: {:?}", &type_id.clone()),
                }
            } else if let Some(SystemSignal::Terminate) = envelope.message.as_any().downcast_ref::<SystemSignal>() {
                trace!(actor=self.key, "Mailbox received {:?} with type_id {:?} for", &envelope.message, &type_id);
                self.terminate().await;
            }
        }
        (self.setup.on_before_stop)(self);
        if let Some(ref on_before_stop_async) = self.setup.on_before_stop_async {
            if timeout(Duration::from_secs(5), on_before_stop_async(self)).await.is_err() {
                tracing::error!("on_before_stop_async timed out or failed");
            }
        }
        (self.setup.on_stop)(self);
    }
    #[instrument(skip(self))]
    async fn terminate(&mut self) {
        tracing::trace!(actor=self.key, "Received SystemSignal::Terminate for");
        for item in &self.actor_ref.children {
            let context = item.value();
            let _ = context.suspend().await;
        }
        for pool in &self.pool_supervisor {
            for item_context in &pool.pool {
                trace!(item=item_context.key,"Terminating pool item.");
                let _ = item_context.suspend().await;
            }
        }
        trace!(actor=self.key,"All subordinates terminated. Closing mailbox for");
        self.inbox.close();
    }
}

/// Represents an actor in the idle state.
///
/// # Type Parameters
/// - `State`: The type representing the state of the actor.
impl<ManagedEntity: Default + Send + Debug + 'static> ManagedActor<Idle<ManagedEntity>, ManagedEntity> {
    /// Creates and supervises a new actor with the given ID and state.
    ///
    /// # Parameters
    /// - `id`: The identifier for the new actor.
    ///
    /// # Returns
    /// A new `Actor` instance in the idle state.
    #[instrument(skip(self))]
    pub async fn create_child(
        &self,
        config: ActorConfig,
    ) -> ManagedActor<Idle<ManagedEntity>, ManagedEntity> {
        let actor = ManagedActor::new(&Some(self.akton.clone()), None, ManagedEntity::default()).await;

        event!(Level::TRACE, new_actor_key = &actor.key);
        actor
    }
    /// Creates a new actor with the given ID, state, and optional parent context.
    ///
    /// # Parameters
    /// - `id`: The identifier for the new actor.
    /// - `state`: The initial state of the actor.
    /// - `parent_context`: An optional parent context for the new actor.
    ///
    /// # Returns
    /// A new `Actor` instance.
    #[instrument(skip(entity))]
    pub(crate) async fn new(akton: &Option<AktonReady>, config: Option<ActorConfig>, entity: ManagedEntity) -> Self {
        let (outbox, inbox) = channel(255);
        let mut actor_ref: ActorRef = Default::default();
        actor_ref.outbox = Some(outbox.clone());

        let mut key = Arn::default().to_string();
        let mut parent = Default::default();
        let mut broker = Default::default();

        if let Some(config) = config {
            key = config.name().clone();
            parent = config.parent().clone();
            if let Some(config_broker) = config.get_broker() {
                broker = config_broker.clone();
                actor_ref.broker = Box::new(Some(config_broker.clone()));
            } else {
                broker = actor_ref.clone();
            }
        } else {
            broker = actor_ref.clone();
        }
        actor_ref.key = key.clone();
        // Ensure the mailbox and outbox are not closed
        debug_assert!(!inbox.is_closed(), "Actor mailbox is closed in new");
        debug_assert!(!outbox.is_closed(), "Outbox is closed in new");

        trace!("NEW ACTOR: {}", &key);
        let akton = {
            if let Some(akton) = akton.clone() {
                akton.clone()
            } else {
                AktonReady {
                    0: AktonInner { broker: broker.clone() },
                }
            }
        };
        // Create and return the new actor instance
        ManagedActor {
            setup: Idle::default(),
            actor_ref,
            parent,
            key,
            entity,
            broker,
            inbox,
            akton,
            ..Default::default()
        }
    }

    #[instrument(skip(self), fields(key = self.key))]
    pub async fn activate(mut self) -> ActorRef {
        let reactors = mem::take(&mut self.setup.reactors);
        let actor_ref = self.actor_ref.clone();

        let active_actor: ManagedActor<Awake<ManagedEntity>, ManagedEntity> = self.into();
        let actor = Box::leak(Box::new(active_actor));

        debug_assert!(!actor.inbox.is_closed(), "Actor mailbox is closed in activate");

        let _ = actor_ref.tracker().spawn(actor.wake(reactors));
        actor_ref.tracker().close();

        actor_ref
    }
}
