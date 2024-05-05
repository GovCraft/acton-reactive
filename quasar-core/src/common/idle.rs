/*
 *
 *  * Copyright (c) 2024 Govcraft.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

use std::any::TypeId;
use std::fmt::Debug;
use std::future::Future;
use std::hash::RandomState;

use std::pin::Pin;
use std::sync::Arc;

use crate::common::event_record::EventRecord;
use crate::common::*;
use crate::common::{LifecycleReactor, SignalReactor};
use crate::traits::{QuasarMessage, SystemMessage};
use dashmap::DashMap;
use futures::future;
use quasar_qrn::Qrn;
use tokio::sync::Mutex;
use tracing::{debug, error, instrument};

pub struct Idle<State: Default + Send + Debug + 'static> {
    pub(crate) on_before_wake: Box<IdleLifecycleReactor<Idle<State>, State>>,
    pub(crate) on_wake: Box<LifecycleReactor<Awake<State>, State>>,
    pub(crate) on_before_stop: Box<LifecycleReactor<Awake<State>, State>>,
    pub(crate) on_stop: Box<LifecycleReactor<Awake<State>, State>>,
    pub(crate) on_before_stop_async: Option<LifecycleReactorAsync<State>>,
    pub(crate) reactors: ReactorMap<State>,
}

impl<State: Default + Send + Debug> Idle<State> {
    #[instrument(skip(self, message_reactor))]
    pub fn act_on<M: QuasarMessage + 'static + Clone>(
        &mut self,
        message_reactor: impl Fn(&mut Actor<Awake<State>, State>, &EventRecord<M>)
            + Send
            + Sync
            + 'static,
    ) -> &mut Self {
        // let message_handler = Arc::new(message_reactor);
        let type_id = TypeId::of::<M>();

        let handler_box: Box<MessageReactor<State>> = Box::new(
            move |actor: &mut Actor<Awake<State>, State>, envelope: &Envelope| {
                if let Some(concrete_msg) = envelope.message.as_any().downcast_ref::<M>() {
                    let cloned_message = concrete_msg.clone(); // Cloning the message
                    let event_record = EventRecord {
                        message: cloned_message,
                        sent_time: envelope.sent_time,
                        return_address: Some(OutboundEnvelope::new(
                            envelope.return_address.clone(),
                            actor.key.clone(),
                        )),
                    };
                    // Here, ensure the future is 'static
                    message_reactor(actor, &event_record);
                    Box::pin(())
                } else {
                    error!(
                        "Message type mismatch: expected {:?}",
                        std::any::type_name::<M>()
                    );
                    unreachable!("Shouldn't get here");
                    // Box::pin(future::ready(())) // Ensure this future is also 'static
                };
            },
        );

        let _ = &self
            .reactors
            .insert(type_id, ReactorItem::Message(handler_box));

        self
    }
    #[instrument(skip(self, message_processor))]
    pub fn act_on_async<M>(
        &mut self,
        message_processor: impl for<'a> Fn(&'a mut Actor<Awake<State>, State>, &'a EventRecord<&'a M>) -> Fut
            + 'static
            + Sync
            + Send,
    ) -> &mut Self
    where
        M: QuasarMessage + 'static,
    {
        let type_id = TypeId::of::<M>();
        let handler_box = Box::new(
            move |actor: &mut Actor<Awake<State>, State>, envelope: &Envelope| -> Fut {
                if let Some(concrete_msg) = envelope.message.as_any().downcast_ref::<M>() {
                    let event_record = EventRecord {
                        message: concrete_msg,
                        sent_time: envelope.sent_time,
                        return_address: actor.parent_return_envelope.clone(),
                    };
                    if let Some(envelope) = &event_record.return_address {
                        // tracing::error!("sender: {}", &envelope.sender);
                    }
                    // Call the user-provided function and get the future
                    let user_future = message_processor(actor, &event_record);

                    // Automatically box and pin the user future
                    Box::pin(user_future)
                } else {
                    // Return an immediately resolving future if downcast fails
                    Box::pin(async {})
                }
            },
        );

        let _ = &self
            .reactors
            .insert(type_id, ReactorItem::Future(handler_box));
        self
    }
    #[instrument(skip(self, signal_reactor))]
    pub fn act_on_internal_signal<M: QuasarMessage + 'static + Clone>(
        &mut self,
        signal_reactor: impl Fn(&mut Actor<Awake<State>, State>, &dyn QuasarMessage) -> Fut
            + Send
            + Sync
            + 'static,
    ) -> &mut Self {
        let type_id = TypeId::of::<M>();

        let handler_box: Box<SignalReactor<State>> = Box::new(
            move |actor: &mut Actor<Awake<State>, State>, message: &dyn QuasarMessage| -> Fut {
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
        let _ = &self
            .reactors
            .insert(type_id, ReactorItem::Signal(handler_box));

        self
    }

    pub fn on_before_wake(
        &mut self,
        life_cycle_event_reactor: impl Fn(&Actor<Idle<State>, State>) + Send + 'static,
    ) -> &mut Self {
        self.on_before_wake = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn on_wake(
        &mut self,
        life_cycle_event_reactor: impl Fn(&Actor<Awake<State>, State>) + Send + 'static,
    ) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_wake = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn on_stop(
        &mut self,
        life_cycle_event_reactor: impl Fn(&Actor<Awake<State>, State>) + Send + 'static,
    ) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_stop = Box::new(life_cycle_event_reactor);
        self
    }
    pub fn on_before_stop(
        &mut self,
        life_cycle_event_reactor: impl Fn(&Actor<Awake<State>, State>) + Send + 'static,
    ) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_before_stop = Box::new(life_cycle_event_reactor);
        self
    }
    pub fn on_before_stop_async<F>(&mut self, f: F) -> &mut Self
    where
        F: for<'a> Fn(&'a Actor<Awake<State>, State>) -> Fut + Send + Sync + 'static,
    {
        self.on_before_stop_async = Some(Box::new(f));
        self
    }

    pub fn new() -> Idle<State>
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

impl<State: Default + Send + Debug + 'static> Default for Idle<State> {
    fn default() -> Self {
        Idle::new()
    }
}
//endregion
