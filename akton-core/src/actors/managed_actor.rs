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

mod idle;
pub mod running;

use std::any::{type_name_of_val, TypeId};
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
pub use idle::Idle;

use crate::common::{ActorRef, Akton, AktonInner, AsyncLifecycleHandler, BrokerRef, FutureBox, HaltSignal, IdleLifecycleHandler, LifecycleHandler, MessageHandler, ParentRef, ReactorItem, ReactorMap, SystemSignal};
use crate::message::{BrokerRequestEnvelope, Envelope, EventRecord, OutboundEnvelope};
use crate::prelude::{AktonMessage, AktonReady};
use crate::traits::Actor;

use super::{ActorConfig, Running};

pub struct ManagedActor<ActorState, ManagedEntity: Default + Send + Debug + 'static> {
    pub actor_ref: ActorRef,

    pub parent: Option<ParentRef>,

    pub broker: BrokerRef,

    pub halt_signal: HaltSignal,

    pub key: String,
    pub akton: AktonReady,

    pub entity: ManagedEntity,

    pub(crate) tracker: TaskTracker,

    pub inbox: Receiver<Envelope>,
    /// Reactor called before the actor wakes up.
    pub(crate) before_activate: Box<IdleLifecycleHandler<Idle, ManagedEntity>>,
    /// Reactor called when the actor wakes up.
    pub(crate) on_activate: Box<LifecycleHandler<Running, ManagedEntity>>,
    /// Reactor called just before the actor stops.
    pub(crate) before_stop: Box<LifecycleHandler<Running, ManagedEntity>>,
    /// Reactor called when the actor stops.
    pub(crate) on_stop: Box<LifecycleHandler<Running, ManagedEntity>>,
    /// Asynchronous reactor called just before the actor stops.
    pub(crate) before_stop_async: Option<AsyncLifecycleHandler<ManagedEntity>>,
    /// Map of reactors for handling different message types.
    pub(crate) reactors: ReactorMap<ManagedEntity>,
    _actor_state: std::marker::PhantomData<ActorState>,

}

impl<ManagedEntity: Default + Send + Debug + 'static> Default for ManagedActor<Idle, ManagedEntity> {
    fn default() -> Self {
        let (outbox, inbox) = channel(255);
        let mut actor_ref: ActorRef = Default::default();
        actor_ref.outbox = Some(outbox.clone());

        ManagedActor::<Idle, ManagedEntity> {
            actor_ref,
            parent: Default::default(),
            key: Default::default(),
            entity: ManagedEntity::default(),
            broker: Default::default(),
            inbox,
            akton: Default::default(),
            halt_signal: Default::default(),
            tracker: Default::default(),
            before_activate: Box::new(|_| {}),
            on_activate: Box::new(|_| {}),
            before_stop: Box::new(|_| {}),
            on_stop: Box::new(|_| {}),
            before_stop_async: None,
            reactors: DashMap::new(),

            _actor_state: Default::default(),
        }
    }
}


impl<ActorState, ManagedEntity: Default + Send + Debug + 'static> Debug for ManagedActor<ActorState, ManagedEntity> {
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
impl<ManagedEntity: Default + Send + Debug + 'static> ManagedActor<Running, ManagedEntity> {
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
    pub(crate) async fn wake(&mut self, reactors: ReactorMap<ManagedEntity>) {
        (self.on_activate)(self);

        while let Some(mut incoming_envelope) = self.inbox.recv().await {
            let type_id;
            let mut envelope;
            debug!("{}",type_name_of_val(&incoming_envelope.message));
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
let is_target = reactors.len() > 0;

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
        (self.before_stop)(self);
        if let Some(ref on_before_stop_async) = self.before_stop_async {
            if timeout(Duration::from_secs(5), on_before_stop_async(self)).await.is_err() {
                tracing::error!("on_before_stop_async timed out or failed");
            }
        }
        (self.on_stop)(self);
    }
    #[instrument(skip(self))]
    async fn terminate(&mut self) {
        tracing::trace!(actor=self.key, "Received SystemSignal::Terminate for");
        for item in &self.actor_ref.children() {
            let child_ref = item.value();
            let _ = child_ref.suspend().await;
        }
        trace!(actor=self.key,"All subordinates terminated. Closing mailbox for");
        self.inbox.close();
    }
}

/// Represents an actor in the idle state.
///
/// # Type Parameters
/// - `State`: The type representing the state of the actor.
impl<ManagedEntity: Default + Send + Debug + 'static> ManagedActor<Idle, ManagedEntity> {
    /// Adds a synchronous message handler for a specific message type.
    ///
    /// # Parameters
    /// - `message_reactor`: The function to handle the message.
    #[instrument(skip(self, message_handler))]
    pub fn act_on<M: AktonMessage + Clone + 'static>(
        &mut self,
        message_handler: impl Fn(&mut ManagedActor<Running, ManagedEntity>, &mut EventRecord<M>)
        + Send
        + Sync
        + 'static,
    ) -> &mut Self {
        let type_id = TypeId::of::<M>();
        trace!(type_name = std::any::type_name::<M>(), type_id = ?type_id);
        // Create a boxed handler for the message type.
        let handler: Box<MessageHandler<ManagedEntity>> = Box::new(
            move |actor: &mut ManagedActor<Running, ManagedEntity>, envelope: &mut Envelope| {
                let envelope_type_id = envelope.message.as_any().type_id();
                info!(
                "Attempting to downcast message: expected_type_id = {:?}, envelope_type_id = {:?}",
                type_id, envelope_type_id
            );
                if let Some(concrete_msg) = downcast_message::<M>(&*envelope.message) {
                    let message = concrete_msg.clone();
                    let sent_time = envelope.sent_time;
                    let return_address = OutboundEnvelope::new(
                        envelope.return_address.clone(),
                        actor.key.clone(),
                    );
                    let event_record = &mut EventRecord {
                        message,
                        sent_time,
                        return_address,
                    };
                    message_handler(actor, event_record);
                    Box::pin(())
                } else {
                    Box::pin({
                        error!(
                        "Message type mismatch: expected {:?}",
                        std::any::type_name::<M>()
                    );
                    })
                };
            },
        );

        // Insert the handler into the reactors map.
        let _ = self.reactors.insert(type_id, ReactorItem::MessageReactor(handler));

        self
    }

    /// Adds an asynchronous message handler for a specific message type.
    ///
    /// # Parameters
    /// - `message_processor`: The function to handle the message.
    #[instrument(skip(self, message_processor))]
    pub fn act_on_async<M>(
        &mut self,
        message_processor: impl for<'a> Fn(&'a mut ManagedActor<Running, ManagedEntity>, &'a mut EventRecord<M>) -> FutureBox
        + Send
        + Sync
        + 'static,
    ) -> &mut Self
    where
        M: AktonMessage + Clone + Send + Sync + 'static,
    {
        let type_id = TypeId::of::<M>();
        trace!(type_name=std::any::type_name::<M>(),type_id=?type_id);
        // Create a boxed handler for the message type.
        let handler_box = Box::new(
            move |actor: &mut ManagedActor<Running, ManagedEntity>, envelope: &mut Envelope| -> FutureBox {
                let envelope_type_id = envelope.message.as_any().type_id();
                info!(
                "Attempting to downcast message: expected_type_id = {:?}, envelope_type_id = {:?}",
                type_id, envelope_type_id
            );
                if let Some(concrete_msg) = downcast_message::<M>(&*envelope.message) {
                    info!("Message successfully downcasted to name {} and concrete type: {:?}",std::any::type_name::<M>(), type_id);

                    let message = concrete_msg.clone();
                    let sent_time = envelope.sent_time;
                    let mut event_record = {
                        if let Some(parent) = &actor.parent {
                            let return_address = parent.return_address();
                            EventRecord {
                                message,
                                sent_time,
                                return_address,
                            }
                        } else {
                            let return_address = actor.actor_ref.return_address();
                            EventRecord {
                                message,
                                sent_time,
                                return_address,
                            }
                        }
                    };

                    // Call the user-provided function and get the future.
                    let user_future = message_processor(actor, &mut event_record);

                    // Automatically box and pin the user future.
                    Box::pin(user_future)
                } else {
                    error!(type_name=std::any::type_name::<M>(),"Should never get here, message failed to downcast");
                    // Return an immediately resolving future if downcast fails.
                    Box::pin(async {})
                }
            },
        );

        // Insert the handler into the reactors map.
        let _ = &self
            .reactors
            .insert(type_id, ReactorItem::FutureReactor(handler_box));
        self
    }


    /// Sets the reactor to be called before the actor wakes up.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn before_activate(
        &mut self,
        life_cycle_event_reactor: impl Fn(&ManagedActor<Idle, ManagedEntity>) + Send + Sync + 'static,
    ) -> &mut Self {
        self.before_activate = Box::new(life_cycle_event_reactor);
        self
    }

    /// Sets the reactor to be called when the actor wakes up.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn on_activate(
        &mut self,
        life_cycle_event_reactor: impl Fn(&ManagedActor<Running, ManagedEntity>) + Send + Sync + 'static,
    ) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_activate = Box::new(life_cycle_event_reactor);
        self
    }

    /// Sets the reactor to be called when the actor stops.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn on_stop(
        &mut self,
        life_cycle_event_reactor: impl Fn(&ManagedActor<Running, ManagedEntity>) + Send + Sync + 'static,
    ) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_stop = Box::new(life_cycle_event_reactor);
        self
    }

    /// Sets the reactor to be called just before the actor stops.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn before_stop(
        &mut self,
        life_cycle_event_reactor: impl Fn(&ManagedActor<Running, ManagedEntity>) + Send + Sync + 'static,
    ) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.before_stop = Box::new(life_cycle_event_reactor);
        self
    }

    /// Sets the asynchronous reactor to be called just before the actor stops.
    ///
    /// # Parameters
    /// - `f`: The asynchronous function to be called.
    pub fn before_stop_async<F>(&mut self, f: F) -> &mut Self
    where
        F: for<'b> Fn(&'b ManagedActor<Running, ManagedEntity>) -> FutureBox + Send + Sync + 'static,
    {
        self.before_stop_async = Some(Box::new(f));
        self
    }


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
    ) -> ManagedActor<Idle, ManagedEntity> {
        let actor = ManagedActor::new(&Some(self.akton.clone()), None, ManagedEntity::default()).await;

        event!(Level::TRACE, new_actor_key = &actor.key);
        actor
    }

    #[instrument(skip(entity))]
    pub(crate) async fn new(akton: &Option<AktonReady>, config: Option<ActorConfig>, entity: ManagedEntity) -> Self {
        let mut managed_actor: ManagedActor<Idle, ManagedEntity> = ManagedActor::default();

        if let Some(config) = &config {
            managed_actor.actor_ref.arn = config.name().clone();
            managed_actor.parent = config.parent().clone();
            managed_actor.actor_ref.broker = Box::new(config.get_broker().clone());
        }

        debug_assert!(!managed_actor.inbox.is_closed(), "Actor mailbox is closed in new");

        trace!("NEW ACTOR: {}", &managed_actor.actor_ref.arn);

        managed_actor.akton = akton.clone().unwrap_or_else(|| AktonReady {
            0: AktonInner { broker: managed_actor.actor_ref.broker.clone().unwrap_or_default() },
        });

        managed_actor.key = managed_actor.actor_ref.arn.clone();

        managed_actor
    }

    #[instrument(skip(self), fields(key = self.key))]
    pub async fn activate(mut self) -> ActorRef {
        let reactors = mem::take(&mut self.reactors);
        let actor_ref = self.actor_ref.clone();

        let active_actor: ManagedActor<Running, ManagedEntity> = self.from_idle();
        let actor = Box::leak(Box::new(active_actor));

        debug_assert!(!actor.inbox.is_closed(), "Actor mailbox is closed in activate");

        let _ = actor_ref.tracker().spawn(actor.wake(reactors));
        actor_ref.tracker().close();

        actor_ref
    }

    #[instrument("from idle to awake", skip(self), fields(
        key = self.key, children_in = self.actor_ref.children().len()
    ))]
    fn from_idle(self) -> ManagedActor<Running, ManagedEntity>
    where
        ManagedEntity: Send + 'static,
    {
        tracing::trace!("*");
        // Extract lifecycle reactors and other properties from the idle actor
        let on_activate = self.on_activate;
        let before_activate = self.before_activate;
        let on_stop = self.on_stop;
        let before_stop = self.before_stop;
        let before_stop_async = self.before_stop_async;
        let halt_signal = self.halt_signal;
        let parent = self.parent;
        let key = self.key;
        let tracker = self.tracker;
        let akton = self.akton;
        let reactors = self.reactors;
        // Trace the process and check if the mailbox is closed before conversion
        tracing::trace!("Checking if mailbox is closed before conversion");
        debug_assert!(
            !self.inbox.is_closed(),
            "Actor mailbox is closed before conversion in From<Actor<Idle, State>>"
        );

        let inbox = self.inbox;
        let actor_ref = self.actor_ref;
        let entity = self.entity;
        let broker = self.broker;

        // Trace the conversion process
        // tracing::trace!(
        //     "Converting Actor from Idle to Awake with key: {}",
        //     key.self
        // );
        // tracing::trace!("Checking if mailbox is closed before conversion");
        debug_assert!(
            !inbox.is_closed(),
            "Actor mailbox is closed in From<Actor<Idle, State>>"
        );

        // tracing::trace!("Mailbox is not closed, proceeding with conversion");
        if actor_ref.children().is_empty() {
            tracing::trace!(
                    "child count before Actor creation {}",
                    actor_ref.children().len()
                );
        }
        // Create and return the new actor in the awake state
        ManagedActor::<Running, ManagedEntity>{
            actor_ref,
            parent,
            halt_signal,
            key,
            akton,
            entity,
            tracker,
            inbox,
            before_activate,
            on_activate,
            before_stop,
            on_stop,
            before_stop_async,
            broker,
            reactors,
            _actor_state: Default::default(),
        }
    }
}

// Function to downcast the message to the original type.
pub fn downcast_message<T: 'static>(msg: &dyn AktonMessage) -> Option<&T> {
    msg.as_any().downcast_ref::<T>()
}
