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

use std::any::{TypeId};
use std::future::Future;

use std::pin::Pin;
use std::sync::{Arc};


use dashmap::DashMap;
use quasar_qrn::Qrn;
use tracing::{debug, error, instrument};
use crate::common::{MessageReactorMap, LifecycleReactor, SignalReactor, SignalReactorMap};
use crate::common::*;
use crate::common::event_record::EventRecord;
use crate::traits::{QuasarMessage, SystemMessage};
use futures::{future};
use tokio::sync::Mutex;


pub struct Idle<T: 'static + Send + Sync> {
    pub key: Qrn,
    pub state: T,
    // Consider using an Arc<U> or similar if lifetimes are a problem
    pub(crate) on_before_wake: Box<IdleLifecycleReactor<Idle<T>>>,
    pub(crate) on_wake: Box<LifecycleReactor<Awake<T>>>,
    pub(crate) on_stop: Box<LifecycleReactor<Awake<T>>>,
    pub(crate) reactors: ReactorMap<T>,
}


impl<T: Default + Send + Sync> Idle<T> {
    // #[instrument(skip(self, message_reactor))]
    // pub type MessageReactor<T, U> = dyn for<'a, 'b> Fn(&mut Actor<Awake<T, U>>, &'b Envelope) + Send + Sync + 'static;
    pub fn act_on<M: QuasarMessage + 'static + Clone>(
        &mut self,
        message_reactor: impl Fn(&mut Actor<Awake<T>>, &EventRecord<M>) + Send + Sync + 'static,
    ) -> &mut Self {
        // let message_handler = Arc::new(message_reactor);
        let type_id = TypeId::of::<M>();

        let handler_box: Box<MessageReactor<T>> = Box::new(move |actor: &mut Actor<Awake<T>>, envelope: &Envelope| {
            if let Some(concrete_msg) = envelope.message.as_any().downcast_ref::<M>() {
                let cloned_message = concrete_msg.clone();  // Cloning the message
                let event_record = EventRecord { message: cloned_message, sent_time: envelope.sent_time };
                // Here, ensure the future is 'static
                message_reactor(actor, &event_record);
                Box::pin(())
            } else {
                error!("Message type mismatch: expected {:?}", std::any::type_name::<M>());
                unreachable!("Shouldn't get here");
                // Box::pin(future::ready(())) // Ensure this future is also 'static
            };
        });

        let _ = &self.reactors.insert(type_id, ReactorItem::Message(handler_box));

        self
    }
    #[instrument(skip(self, message_processor))]
    pub fn act_on_async<M>(&mut self, message_processor: impl for<'a> Fn(&'a mut Actor<Awake<T>>, &'a EventRecord<&'a M>) -> Pin<Box<dyn Future<Output=()> + Send + Sync>> + 'static + Send + Sync) -> &mut Self
        where M: QuasarMessage + 'static
    {
        let type_id = TypeId::of::<M>();
        let handler_box = Box::new(move |actor: &mut Actor<Awake<T>>, envelope: &Envelope| -> Pin<Box<dyn Future<Output=()> + Send>> {
            if let Some(concrete_msg) = envelope.message.as_any().downcast_ref::<M>() {
                let event_record = EventRecord { message: concrete_msg, sent_time: envelope.sent_time };

                // Call the user-provided function and get the future
                let user_future = message_processor(actor, &event_record);

                // Automatically box and pin the user future
                Box::pin(user_future)
            } else {
                // Return an immediately resolving future if downcast fails
                Box::pin(async {})
            }
        });

        let _ = &self.reactors.insert(type_id, ReactorItem::Future(handler_box));
        self
    }
    #[instrument(skip(self, signal_reactor))]
    pub fn act_on_internal_signal<M: QuasarMessage + 'static + Clone>(
        &mut self,
        signal_reactor: impl Fn(Actor<Awake<T>>, &dyn QuasarMessage) -> Pin<Box<dyn Future<Output=()> + Send + Sync>> + Send + Sync + 'static,
    ) -> &mut Self {
        let type_id = TypeId::of::<M>();

        let handler_box: Box<SignalReactor<T>> = Box::new(move |actor: Actor<Awake<T>>, message: &dyn QuasarMessage| {
            if let Some(concrete_msg) = message.as_any().downcast_ref::<M>() {
                signal_reactor(actor, concrete_msg)
            } else {
                error!("Message type mismatch: expected {:?}", std::any::type_name::<M>());
                Box::pin(future::ready(()))
            }
        });

        debug!("adding signal reactor to reactors");
        let _ = &self.reactors.insert(type_id, ReactorItem::Signal(handler_box));

        self
    }

    // pub fn act_on_internal_signal<M: SystemMessage + 'static>(&mut self, signal_reactor: impl Fn(Arc<Mutex<Awake<T, U>>>, &M) + Send + Sync + 'static) -> &mut Self {
    //     // Create a boxed handler that can be stored in the HashMap.
    //     let signal_reactor_box: SignalReactor<T, U> = Box::new(move |actor: Arc<Mutex<Awake<T, U>>>, system_message: &dyn SystemMessage| {
    //         // Attempt to downcast the message to its concrete type.
    //         if let Some(concrete_msg) = system_message.as_any().downcast_ref::<M>() {
    //             signal_reactor(actor, concrete_msg);
    //         } else {
    //             // If downcasting fails, log a warning.
    //             error!("Warning: SystemMessage type mismatch: expected type {:?}", std::any::type_name::<M>());
    //         }
    //     });
    //
    //     // Use the type ID of the concrete message type M as the key in the handlers map.
    //     let type_id = TypeId::of::<M>();
    //     self.signal_reactors.insert(type_id, signal_reactor_box);
    //
    //     // Return self to allow chaining.
    //     self
    // }


    pub fn on_before_wake(&mut self, life_cycle_event_reactor:  impl Fn(&Actor<Idle<T>>) + Send + Sync + 'static) -> &mut Self {
        self.on_before_wake = Box::new(life_cycle_event_reactor);
        self
    }


    pub fn on_wake(&mut self, life_cycle_event_reactor: impl Fn(&Actor<Awake<T>>) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_wake = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn on_stop(&mut self, life_cycle_event_reactor: impl Fn(&Actor<Awake<T>>) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_stop = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn new(qrn: Qrn, state: T) -> Idle<T> {
        Idle {
            key: qrn,
            state,
            on_before_wake: Box::new(|_| {}),
            on_wake: Box::new(|_| {}),
            on_stop: Box::new(|_| {}),
            reactors: DashMap::new(),
        }
    }
}
//endregion

