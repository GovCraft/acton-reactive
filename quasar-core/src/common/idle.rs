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
use tracing::{error, instrument, trace};
use crate::common::{MessageReactorMap, LifecycleReactor, SignalReactor, SignalReactorMap};
use crate::common::*;
use crate::common::event_record::EventRecord;
use crate::traits::{QuasarMessage, SystemMessage};
use futures::{future};
use tokio::sync::Mutex;


pub struct Idle<T: 'static + Send + Sync, U: 'static + Send + Sync> {
    pub key: Qrn,
    pub state: T,
    pub parent: Option<&'static U>,  // Consider using an Arc<U> or similar if lifetimes are a problem
    pub(crate) on_before_wake: Box<IdleLifecycleReactor<Idle<T, U>>>,
    pub(crate) on_wake: Box<LifecycleReactor<Awake<T, U>>>,
    pub(crate) on_stop: Box<LifecycleReactor<Awake<T, U>>>,
    pub(crate) message_reactors: MessageReactorMap<T, U>,
    pub(crate) signal_reactors: SignalReactorMap<T, U>,
}


impl<T: Default + Send + Sync, U: Send + Sync> Idle<T, U> {
    #[instrument(skip(self, message_reactor))]
    pub fn act_on<M: QuasarMessage + 'static + Clone>(
        &mut self,
        message_reactor: impl Fn(Arc<Mutex<Awake<T, U>>>, &EventRecord<M>) -> Pin<Box<dyn Future<Output=()> + Sync + Send>> + Sync + Send + 'static,
    ) -> &mut Self {
        trace!("entering message reactors");
        let type_id = TypeId::of::<M>();
        let handler_box: Box<MessageReactor<T, U>> = Box::new(move |actor: Arc<Mutex<Awake<T, U>>>, envelope: &Envelope| {
            if let Some(concrete_msg) = envelope.message.as_any().downcast_ref::<M>() {
                let cloned_message = concrete_msg.clone();
                let event_record = EventRecord { message: cloned_message, sent_time: envelope.sent_time };
                message_reactor(actor, &event_record)
            } else {
                error!("Message type mismatch: expected {:?}", std::any::type_name::<M>());
                Box::pin(future::ready(()))
            }
        });
        trace!("Inserting message reactors");
        self.message_reactors.insert(type_id, handler_box);

        self
    }

    pub fn act_on_internal_signal<M: SystemMessage + 'static + Clone>(
        &mut self,
        signal_reactor: impl Fn(Arc<Mutex<Awake<T, U>>>, &dyn SystemMessage) -> Pin<Box<dyn Future<Output=()> + Send + Sync>> + Send + Sync + 'static,
    ) -> &mut Self {
        let type_id = TypeId::of::<M>();

        let handler_box: Box<SignalReactor<T, U>> = Box::new(move |actor: Arc<Mutex<Awake<T, U>>>, message: &dyn SystemMessage| {
            if let Some(concrete_msg) = message.as_any().downcast_ref::<M>() {

                signal_reactor(actor, &*concrete_msg)
            } else {
                error!("Message type mismatch: expected {:?}", std::any::type_name::<M>());
                Box::pin(future::ready(()))
            }
        });

        self.signal_reactors.insert(type_id, handler_box);

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


    pub fn on_before_wake(&mut self, life_cycle_event_reactor: Box<IdleLifecycleReactor<Idle<T, U>>>)  -> &mut Self {
        self.on_before_wake = Box::new(life_cycle_event_reactor);
        self
    }


    pub fn on_wake(&mut self, life_cycle_event_reactor: Box<LifecycleReactor<Awake<T, U>>>) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_wake = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn on_stop(&mut self, life_cycle_event_reactor: impl Fn(Arc<Mutex<Awake<T, U>>>) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_stop = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn new(qrn: Qrn, state: T) -> Idle<T, U> {
        Idle {
            key: qrn,
            state,
            parent: None,
            on_before_wake: Box::new(|_|{}),
            on_wake: Box::new(|_|{}),
            on_stop: Box::new(|_|{}),
            message_reactors: DashMap::new(),
            signal_reactors: DashMap::new(),
        }
    }
}
//endregion

