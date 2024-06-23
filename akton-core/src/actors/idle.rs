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

use crate::actors::{Actor, ActorConfig, Awake};
use crate::common::*;
use crate::common::LifecycleReactor;
use crate::message::{Envelope, EventRecord, OutboundEnvelope};
use crate::traits::{ActorContext, AktonMessage};

/// Represents the lifecycle state of an actor when it is idle.
///
/// # Type Parameters
/// - `State`: The type representing the state of the actor.
pub struct Idle<State: Default + Send + Debug + 'static> {
    /// Reactor called before the actor wakes up.
    pub(crate) on_before_wake: Box<IdleLifecycleReactor<Idle<State>, State>>,
    /// Reactor called when the actor wakes up.
    pub(crate) on_wake: Box<LifecycleReactor<Awake<State>, State>>,
    /// Reactor called just before the actor stops.
    pub(crate) on_before_stop: Box<LifecycleReactor<Awake<State>, State>>,
    /// Reactor called when the actor stops.
    pub(crate) on_stop: Box<LifecycleReactor<Awake<State>, State>>,
    /// Asynchronous reactor called just before the actor stops.
    pub(crate) on_before_stop_async: Option<AsyncLifecycleReactor<State>>,
    /// Map of reactors for handling different message types.
    pub(crate) reactors: ReactorMap<State>,
}

/// Custom implementation of the `Debug` trait for the `Idle` struct.
///
/// This implementation provides a formatted output for the `Idle` struct.
impl<State: Default + Send + Debug + 'static> Debug for Idle<State> {
    /// Formats the `Idle` struct using the given formatter.
    ///
    /// # Parameters
    /// - `f`: The formatter used for writing formatted output.
    ///
    /// # Returns
    /// A result indicating whether the formatting was successful.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Idle").finish()
    }
}

/// Represents an actor in the idle state and provides methods to set up its behavior.
///
/// # Type Parameters
/// - `State`: The type representing the state of the actor.
impl<State: Default + Send + Debug> Idle<State> {


    /// Adds a synchronous message handler for a specific message type.
    ///
    /// # Parameters
    /// - `message_reactor`: The function to handle the message.
    #[instrument(skip(self, message_reactor))]
    pub fn act_on<M: AktonMessage + Clone + 'static>(
        &mut self,
        message_reactor: impl Fn(&mut Actor<Awake<State>, State>, &mut EventRecord<M>)
        + Send
        + Sync
        + 'static,
    ) -> &mut Self {
        let type_id = TypeId::of::<M>();
        trace!(type_name=std::any::type_name::<M>(),type_id=?type_id);
        // Create a boxed handler for the message type.
        let handler_box: Box<MessageReactor<State>> = Box::new(
            move |actor: &mut Actor<Awake<State>, State>, envelope: &mut Envelope| {
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
                    message_reactor(actor, event_record);
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
        let _ = &self
            .reactors
            .insert(type_id, ReactorItem::Message(handler_box));

        self
    }

    /// Adds an asynchronous message handler for a specific message type.
    ///
    /// # Parameters
    /// - `message_processor`: The function to handle the message.
    #[instrument(skip(self, message_processor))]
    pub fn act_on_async<M>(
        &mut self,
        message_processor: impl for<'a> Fn(&'a mut Actor<Awake<State>, State>, &'a mut EventRecord<M>) -> Fut
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
            move |actor: &mut Actor<Awake<State>, State>, envelope: &mut Envelope| -> Fut {
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
                            let return_address = parent.get_return_address();
                            EventRecord {
                                message,
                                sent_time,
                                return_address,
                            }
                        } else {
                            let return_address = actor.context.get_return_address();
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
            .insert(type_id, ReactorItem::Future(handler_box));
        self
    }

    /// Adds an internal signal handler for a specific signal type.
    ///
    /// # Parameters
    /// - `signal_reactor`: The function to handle the signal.
    #[instrument(skip(self, signal_reactor))]
    pub fn act_on_internal_signal<M: AktonMessage + 'static + Clone>(
        &mut self,
        signal_reactor: impl Fn(&mut Actor<Awake<State>, State>, &dyn AktonMessage) -> Fut
        + Send
        + Sync
        + 'static,
    ) -> &mut Self {
        let type_id = TypeId::of::<M>();

        // Create a boxed handler for the signal type.
        let handler_box: Box<SignalReactor<State>> = Box::new(
            move |actor: &mut Actor<Awake<State>, State>, message: &dyn AktonMessage| -> Fut {
                if let Some(concrete_msg) = message.as_any().downcast_ref::<M>() {
                    let fut = signal_reactor(actor, concrete_msg);
                    Box::pin(fut)
                } else {
                    error!(
                        "Message type mismatch: expected {:?}",
                        std::any::type_name::<M>()
                    );
                    Box::pin(future::ready(()))
                }
            },
        );

        debug!("adding signal reactor to reactors");
        // Insert the handler into the reactors map.
        let _ = &self
            .reactors
            .insert(type_id, ReactorItem::Signal(handler_box));

        self
    }

    /// Sets the reactor to be called before the actor wakes up.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn on_before_wake(
        &mut self,
        life_cycle_event_reactor: impl Fn(&Actor<Idle<State>, State>) + Send + Sync + 'static,
    ) -> &mut Self {
        self.on_before_wake = Box::new(life_cycle_event_reactor);
        self
    }

    /// Sets the reactor to be called when the actor wakes up.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn on_wake(
        &mut self,
        life_cycle_event_reactor: impl Fn(&Actor<Awake<State>, State>) + Send + Sync + 'static,
    ) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_wake = Box::new(life_cycle_event_reactor);
        self
    }

    /// Sets the reactor to be called when the actor stops.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn on_stop(
        &mut self,
        life_cycle_event_reactor: impl Fn(&Actor<Awake<State>, State>) + Send + Sync + 'static,
    ) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_stop = Box::new(life_cycle_event_reactor);
        self
    }

    /// Sets the reactor to be called just before the actor stops.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn on_before_stop(
        &mut self,
        life_cycle_event_reactor: impl Fn(&Actor<Awake<State>, State>) + Send + Sync + 'static,
    ) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_before_stop = Box::new(life_cycle_event_reactor);
        self
    }

    /// Sets the asynchronous reactor to be called just before the actor stops.
    ///
    /// # Parameters
    /// - `f`: The asynchronous function to be called.
    pub fn on_before_stop_async<F>(&mut self, f: F) -> &mut Self
    where
        F: for<'b> Fn(&'b Actor<Awake<State>, State>) -> Fut + Send + Sync + 'static,
    {
        self.on_before_stop_async = Some(Box::new(f));
        self
    }

    /// Creates a new idle actor with default lifecycle reactors.
    ///
    /// # Returns
    /// A new `Idle` instance with default settings.
    pub(crate) fn new() -> Idle<State>
    where
        State: Send + 'static,
    {
        Idle {
            on_before_wake: Box::new(|_| {}),
            on_wake: Box::new(|_| {}),
            on_before_stop: Box::new(|_| {}),
            on_stop: Box::new(|_| {}),
            on_before_stop_async: None,
            reactors: DashMap::new(),
        }
    }
}

/// Provides a default implementation for the `Idle` struct.
///
/// This implementation creates a new `Idle` instance with default settings.
impl<State: Default + Send + Debug + 'static> Default for Idle<State> {
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
