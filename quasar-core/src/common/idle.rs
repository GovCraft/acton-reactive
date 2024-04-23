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



use dashmap::DashMap;
use quasar_qrn::Qrn;
use tracing::{error, instrument};
use crate::common::{MessageReactorMap, LifecycleReactor, SignalReactor, SignalReactorMap};
use crate::common::*;
use crate::common::event_record::EventRecord;
use crate::traits::{ QuasarMessage, SystemMessage};
use futures::{future};


pub struct Idle<T: 'static + Send + Sync, U: 'static + Send + Sync> {
    pub key: Qrn,
    pub state: T,
    pub parent: Option<&'static U>,
    pub(crate) on_before_wake: LifecycleReactor<Self>,
    pub(crate) on_wake: LifecycleReactor<Awake<T, U>>,
    pub(crate) on_stop: LifecycleReactor<Awake<T, U>>,
    pub(crate) message_reactors: MessageReactorMap<T, U>,
    pub(crate) signal_reactors: SignalReactorMap<T, U>,
}

impl<T: Default + Send + Sync, U: Send + Sync> Idle<T, U> {
    #[instrument(skip(self, message_reactor))]
    pub fn act_on<M: QuasarMessage + 'static + Clone, G: Send + 'static>(
        &mut self,
        message_reactor: impl Fn(&mut Awake<T, U>, &EventRecord<M>) -> G + Send + 'static
    )  -> &mut Self where G: Future<Output=()> {
        // let message_handler = Arc::new(message_reactor);
        let type_id = TypeId::of::<M>();

        let handler_box: Box<MessageReactor<T, U>> = Box::new(move |actor: &mut Awake<T, U>, envelope: &Envelope| {
            if let Some(concrete_msg) = envelope.message.as_any().downcast_ref::<M>() {
                let cloned_message = concrete_msg.clone();  // Cloning the message
                let event_record = EventRecord { message: cloned_message, sent_time: envelope.sent_time };
                // Here, ensure the future is 'static
                Box::pin(message_reactor(actor, &event_record))
            } else {
                error!("Message type mismatch: expected {:?}", std::any::type_name::<M>());
                Box::pin(future::ready(())) // Ensure this future is also 'static
            }
        });

        let map = &self.message_reactors;
        map.insert(type_id, handler_box);

        self
    }


    pub fn observe_singularity_signal<M: SystemMessage + 'static>(&mut self, lifecycle_message_reactor: impl Fn(&mut Awake<T, U>, &M) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        let lifecycle_message_reactor_box: SignalReactor<T, U> = Box::new(move |actor: &mut Awake<T, U>, lifecycle_message: &dyn SystemMessage| {
            // Attempt to downcast the message to its concrete type.
            if let Some(concrete_msg) = lifecycle_message.as_any().downcast_ref::<M>() {
                lifecycle_message_reactor(actor, concrete_msg);
            } else {
                // If downcasting fails, log a warning.
                error!("Warning: SystemMessage type mismatch: {:?}", std::any::type_name::<M>());
            }
        });

        // Use the type ID of the concrete message type M as the key in the handlers map.
        let type_id = TypeId::of::<M>();
        self.signal_reactors.insert(type_id, lifecycle_message_reactor_box);

        // Return self to allow chaining.
        self
    }

    pub fn on_before_wake(&mut self, life_cycle_event_reactor: impl Fn(&Idle<T, U>) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_before_wake = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn on_wake(&mut self, life_cycle_event_reactor: impl Fn(&Awake<T, U>) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_wake = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn on_stop(&mut self, life_cycle_event_reactor: impl Fn(&Awake<T, U>) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_stop = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn new(qrn: Qrn, state: T) -> Idle<T, U> {
        Idle {
            key: qrn,
            state,
            parent: None,
            on_before_wake: Box::new(|_| {}),
            on_wake: Box::new(|_| {}),
            on_stop: Box::new(|_| {}),
            message_reactors: DashMap::new(),
            signal_reactors: DashMap::new(),
        }
    }
}
//endregion

