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

use crate::common::{ActorRef, Akton, AktonInner, BrokerRef, ParentRef, ReactorItem, ReactorMap, HaltSignal, SystemSignal};
use crate::message::{BrokerRequestEnvelope, Envelope, OutboundEnvelope};
use crate::pool::{PoolBuilder, PoolItem};
use crate::prelude::AktonReady;
use crate::traits::Actor;

use super::{ActorConfig, Awake, Idle};

pub struct ManagedActor<RefType: Send + 'static, ManagedEntity: Default + Send + Debug + 'static> {
    pub setup: RefType,

    pub actor_ref: ActorRef,

    pub parent: Option<ParentRef>,

    pub broker: BrokerRef,

    pub halt_signal: HaltSignal,

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
            setup: Idle::default(),
            actor_ref,
            parent: Default::default(),
            key: Default::default(),
            entity: ManagedEntity::default(),
            broker: Default::default(),
            inbox,
            akton: Default::default(),
            halt_signal: Default::default(),
            tracker: Default::default(),
            pool_supervisor: Default::default(),
        }
    }
}


impl<RefType: Send + 'static, State: Default + Send + Debug + 'static> Debug for ManagedActor<RefType, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedActor")
            .field("key", &self.key)
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
                );
                type_id = broker_request_envelope.message.as_any().type_id().clone();
            } else {
                envelope = incoming_envelope;
                type_id = envelope.message.as_any().type_id().clone();
            }

            if let Some(reactor) = reactors.get(&type_id) {
                match reactor.value() {
                    ReactorItem::MessageReactor(reactor) => (*reactor)(self, &mut envelope),
                    ReactorItem::FutureReactor(fut) => fut(self, &mut envelope).await,
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
            let child_ref = item.value();
            let _ = child_ref.suspend().await;
        }
        for pool in &self.pool_supervisor {
            for pool_item_ref in &pool.pool {
                trace!(item=pool_item_ref.key,"Terminating pool item.");
                let _ = pool_item_ref.suspend().await;
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

    #[instrument(skip(entity))]
    pub(crate) async fn new(akton: &Option<AktonReady>, config: Option<ActorConfig>, entity: ManagedEntity) -> Self {
        let mut managed_actor: ManagedActor<Idle<ManagedEntity>, ManagedEntity> = ManagedActor::default();

        if let Some(config) = &config {
            managed_actor.actor_ref.key = config.name().clone();
            managed_actor.parent = config.parent().clone();
            managed_actor.actor_ref.broker = Box::new(config.get_broker().clone());
        }

        debug_assert!(!managed_actor.inbox.is_closed(), "Actor mailbox is closed in new");

        trace!("NEW ACTOR: {}", &managed_actor.actor_ref.key);

        managed_actor.akton = akton.clone().unwrap_or_else(|| AktonReady {
            0: AktonInner { broker: managed_actor.actor_ref.broker.clone().unwrap_or_default() },
        });

        managed_actor.key = managed_actor.actor_ref.key.clone();

        managed_actor
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
