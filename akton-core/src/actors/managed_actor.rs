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

use crate::common::{Akton, AktonInner, BrokerContext, ActorRef, ParentContext, ReactorItem, ReactorMap, StopSignal, SystemSignal};
use crate::message::{BrokerRequestEnvelope, Envelope, OutboundEnvelope};
use crate::pool::{PoolBuilder, PoolItem};
use crate::prelude::AktonReady;
use crate::traits::Actor;

use super::{ActorConfig, Awake, Idle};

/// Represents an actor in the Akton framework.
///
/// # Type Parameters
/// - `RefType`: The type used for the actor's setup reference.
/// - `State`: The type representing the state of the actor.
pub struct ManagedActor<RefType: Send + 'static, ManagedEntity: Default + Send + Debug + 'static> {
    /// The setup reference for the actor.
    pub setup: RefType,

    /// The context in which the actor operates.
    pub context: ActorRef,

    /// The parent actor's return envelope.
    pub parent: Option<ParentContext>,

    /// The actor's optional context ref to a broker actor.
    pub broker: BrokerContext,

    /// The signal used to halt the actor.
    pub halt_signal: StopSignal,

    /// The unique identifier (ARN) for the actor.
    pub key: String,
    pub akton: AktonReady,

    /// The state of the actor.
    pub state: ManagedEntity,

    /// The task tracker for the actor.
    pub(crate) task_tracker: TaskTracker,

    /// The mailbox for receiving envelopes.
    pub mailbox: Receiver<Envelope>,
    /// The mailbox for receiving envelopes.
    pub(crate) pool_supervisor: DashMap<String, PoolItem>,
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
        if let Some(envelope) = &self.context.outbox {
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

        while let Some(mut incoming_envelope) = self.mailbox.recv().await {
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
                self.terminate_actor().await;
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
    async fn terminate_actor(&mut self) {
        tracing::trace!(actor=self.key, "Received SystemSignal::Terminate for");
        for item in &self.context.children {
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
        self.mailbox.close();
    }
}

/// Represents an actor in the idle state.
///
/// # Type Parameters
/// - `State`: The type representing the state of the actor.
impl<State: Default + Send + Debug + 'static> ManagedActor<Idle<State>, State> {
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
    ) -> ManagedActor<Idle<State>, State> {
        let actor = ManagedActor::new(&Some(self.akton.clone()), None, State::default()).await;

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
    #[instrument(skip(state))]
    pub(crate) async fn new(akton: &Option<AktonReady>, config: Option<ActorConfig>, state: State) -> Self {
        // Create a channel with a buffer size of 255 for the actor's mailbox
        let (outbox, mailbox) = channel(255);
        let mut context: ActorRef = Default::default();
        context.outbox = Some(outbox.clone());

        let mut key = Arn::default().to_string();
        let mut parent = Default::default();
        let mut broker = Default::default();
        let task_tracker = Default::default();

        if let Some(config) = config {
            key = config.name().clone();
            parent = config.parent().clone();
            if let Some(config_broker) = config.get_broker() {
                broker = config_broker.clone();
                context.broker = Box::new(Some(config_broker.clone()));
            } else {
                broker = context.clone();
            }
        } else {
            broker = context.clone();
        }
        context.key = key.clone();
        // Ensure the mailbox and outbox are not closed
        debug_assert!(!mailbox.is_closed(), "Actor mailbox is closed in new");
        debug_assert!(!outbox.is_closed(), "Outbox is closed in new");

        trace!("NEW ACTOR: {}", &key);
        let akton = {
            if let Some(akton) = akton.clone() {
akton.clone()
            } else {
             AktonReady{
                 0: AktonInner { broker: broker.clone() },
             }
            }
        };
        // Create and return the new actor instance
        ManagedActor {
            setup: Idle::default(),
            context,
            parent,
            halt_signal: Default::default(),
            key,
            state,
            broker,
            task_tracker,
            mailbox,
            akton,
            pool_supervisor: Default::default(),
        }
    }

    /// Activates the actor, optionally with a pool builder.
    ///
    /// # Parameters
    /// - `builder`: An optional `PoolBuilder` to initialize a `PoolSupervisor` to manage pool items .
    ///
    /// # Returns
    /// The actor's context after activation.
    #[instrument(skip(self), fields(key = self.key))]
    pub async fn activate(
        self,
        builder: Option<PoolBuilder>,
    ) -> ActorRef {
        // Box::pin(async move {
        // Store and activate all supervised children if a builder is provided
        let mut actor = self;
        let reactors = mem::take(&mut actor.setup.reactors);
        let context = actor.context.clone();


        // If a pool builder is provided, spawn the supervisor
        // if let Some(builder) = builder {
        //     trace!(id = actor.key, "PoolBuilder provided.");
        //     let moved_context = actor.context.clone();
        //     actor.pool_supervisor = builder.spawn(&moved_context).await?;
        // }

        // here we transition from an Actor<Idle> to an Actor<Awake>
        let active_actor: ManagedActor<Awake<State>, State> = actor.into();

        // makes actor live for static, required for the `wake` function
        let actor = Box::leak(Box::new(active_actor));
        debug_assert!(
            !actor.mailbox.is_closed(),
            "Actor mailbox is closed in spawn"
        );

        // TODO: we need to store this join handle
        // Spawn the actor's wake task
        let _ = &context.task_tracker.spawn(actor.wake(reactors));

        context.task_tracker.close();

        context.clone()
        // })
    }
}

/// Represents an actor in the awake state.
///
/// # Type Parameters
/// - `State`: The type representing the state of the actor.
impl<State: Default + Send + Debug + 'static> ManagedActor<Awake<State>, State> {
    /// Terminates the actor by setting the halt signal.
    ///
    /// This method sets the halt signal to true, indicating that the actor should stop processing.
    #[instrument(skip(self))]
    pub(crate) fn terminate(&self) {
        // Load the current value of the halt signal with sequential consistency ordering
        self.halt_signal.load(Ordering::SeqCst);

        // Store `true` in the halt signal to indicate termination
        self.halt_signal.store(true, Ordering::SeqCst);
    }
}
