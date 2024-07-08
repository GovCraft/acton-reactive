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

use std::any::TypeId;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use dashmap::DashMap;
use futures::future;
use tracing::*;

use crate::actors::{ManagedActor, ActorConfig, Awake};
use crate::common::*;
use crate::common::LifecycleHandler;
use crate::message::{Envelope, EventRecord, OutboundEnvelope};
use crate::traits::{Actor, AktonMessage};

/// Represents the lifecycle state of an actor when it is idle.
///
/// # Type Parameters
/// - `ManagedEntity`: The type representing the state of the actor.
pub struct Idle<ManagedEntity: Default + Send + Debug + 'static> {
    /// Reactor called before the actor wakes up.
    pub(crate) before_wake: Box<IdleLifecycleHandler<Idle<ManagedEntity>, ManagedEntity>>,
    /// Reactor called when the actor wakes up.
    pub(crate) wake: Box<LifecycleHandler<Awake<ManagedEntity>, ManagedEntity>>,
    /// Reactor called just before the actor stops.
    pub(crate) before_stop: Box<LifecycleHandler<Awake<ManagedEntity>, ManagedEntity>>,
    /// Reactor called when the actor stops.
    pub(crate) stop: Box<LifecycleHandler<Awake<ManagedEntity>, ManagedEntity>>,
    /// Asynchronous reactor called just before the actor stops.
    pub(crate) before_stop_async: Option<AsyncLifecycleHandler<ManagedEntity>>,
    /// Map of reactors for handling different message types.
    pub(crate) reactors: ReactorMap<ManagedEntity>,
}

impl<ManagedEntity: Default + Send + Debug + 'static> Debug for Idle<ManagedEntity> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Idle")
         .field("before_wake", &"...")
         .field("wake", &"...")
         .field("before_stop", &"...")
         .field("stop", &"...")
         .field("before_stop_async", &self.before_stop_async.is_some())
         .field("reactors", &self.reactors.len())
         .finish()
    }
}

/// Represents an actor in the idle state and provides methods to set up its behavior.
///
/// # Type Parameters
/// - `ManagedEntity`: The type representing the state of the actor.
impl<ManagedEntity: Default + Send + Debug> Idle<ManagedEntity> {


    /// Adds a synchronous message handler for a specific message type.
    ///
    /// # Parameters
    /// - `message_reactor`: The function to handle the message.
    #[instrument(skip(self, message_handler))]
    pub fn act_on<M: AktonMessage + Clone + 'static>(
        &mut self,
        message_handler: impl Fn(&mut ManagedActor<Awake<ManagedEntity>, ManagedEntity>, &mut EventRecord<M>)
        + Send
        + Sync
        + 'static,
    ) -> &mut Self {
        let type_id = TypeId::of::<M>();
        trace!(type_name = std::any::type_name::<M>(), type_id = ?type_id);
        // Create a boxed handler for the message type.
        let handler: Box<MessageHandler<ManagedEntity>> = Box::new(
            move |actor: &mut ManagedActor<Awake<ManagedEntity>, ManagedEntity>, envelope: &mut Envelope| {
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
        message_processor: impl for<'a> Fn(&'a mut ManagedActor<Awake<ManagedEntity>, ManagedEntity>, &'a mut EventRecord<M>) -> FutureBox
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
            move |actor: &mut ManagedActor<Awake<ManagedEntity>, ManagedEntity>, envelope: &mut Envelope| -> FutureBox {
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
    pub fn before_wake(
        &mut self,
        life_cycle_event_reactor: impl Fn(&ManagedActor<Idle<ManagedEntity>, ManagedEntity>) + Send + Sync + 'static,
    ) -> &mut Self {
        self.before_wake = Box::new(life_cycle_event_reactor);
        self
    }

    /// Sets the reactor to be called when the actor wakes up.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn on_wake(
        &mut self,
        life_cycle_event_reactor: impl Fn(&ManagedActor<Awake<ManagedEntity>, ManagedEntity>) + Send + Sync + 'static,
    ) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.wake = Box::new(life_cycle_event_reactor);
        self
    }

    /// Sets the reactor to be called when the actor stops.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn on_stop(
        &mut self,
        life_cycle_event_reactor: impl Fn(&ManagedActor<Awake<ManagedEntity>, ManagedEntity>) + Send + Sync + 'static,
    ) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.stop = Box::new(life_cycle_event_reactor);
        self
    }

    /// Sets the reactor to be called just before the actor stops.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn on_before_stop(
        &mut self,
        life_cycle_event_reactor: impl Fn(&ManagedActor<Awake<ManagedEntity>, ManagedEntity>) + Send + Sync + 'static,
    ) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.before_stop = Box::new(life_cycle_event_reactor);
        self
    }

    /// Sets the asynchronous reactor to be called just before the actor stops.
    ///
    /// # Parameters
    /// - `f`: The asynchronous function to be called.
    pub fn on_before_stop_async<F>(&mut self, f: F) -> &mut Self
    where
        F: for<'b> Fn(&'b ManagedActor<Awake<ManagedEntity>, ManagedEntity>) -> FutureBox + Send + Sync + 'static,
    {
        self.before_stop_async = Some(Box::new(f));
        self
    }

    /// Creates a new idle actor with default lifecycle reactors.
    ///
    /// # Returns
    /// A new `Idle` instance with default settings.
    pub(crate) fn new() -> Idle<ManagedEntity>
    where
        ManagedEntity: Send + 'static,
    {
        Idle {
            before_wake: Box::new(|_| {}),
            wake: Box::new(|_| {}),
            before_stop: Box::new(|_| {}),
            stop: Box::new(|_| {}),
            before_stop_async: None,
            reactors: DashMap::new(),
        }
    }
}

/// Provides a default implementation for the `Idle` struct.
///
/// This implementation creates a new `Idle` instance with default settings.
impl<ManagedEntity: Default + Send + Debug + 'static> Default for Idle<ManagedEntity> {
    /// Creates a new `Idle` instance with default settings.
    ///
    /// # Returns
    /// A new `Idle` instance.
    fn default() -> Self {
        Idle::new()
    }
}

// Function to downcast the message to the original type.
pub fn downcast_message<T: 'static>(msg: &dyn AktonMessage) -> Option<&T> {
    msg.as_any().downcast_ref::<T>()
}
