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
use dashmap::DashMap;
use quasar_qrn::Qrn;
use tracing::{error, instrument, trace};
use crate::common::{MessageReactor, MessageReactorMap, LifecycleReactor, SignalReactor, SignalReactorMap};
use crate::common::*;
use crate::traits::{QuasarMessage, SystemMessage};

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

impl<T: std::default::Default + Send + Sync, U: Send + Sync> Idle<T, U> {
    #[instrument(skip(self, message_reactor))]
    pub fn act_on<M: QuasarMessage + 'static>(&mut self, message_reactor: impl Fn(&mut Awake<T, U>, &M) + Send + Sync + 'static) -> &mut Self
        where T: Default + Send,
              U: Send {
        // Extract the necessary data from self before moving it into the closure
        let qrn_value = self.key.value.clone(); // Assuming `qrn.value` is cloneable
        trace!("QRN value: {}", qrn_value);
        // Create a boxed reactor that can be stored in the HashMap.
        let message_reactor_box: MessageReactor<T, U> = Box::new(move |actor: &mut Awake<T, U>, message: &dyn QuasarMessage| {
            // Attempt to downcast the message to its concrete type.
            if let Some(concrete_msg) = message.as_any().downcast_ref::<M>() {
                message_reactor(actor, concrete_msg);
            } else {
                // If downcasting fails, log a warning.
                error!("Warning: Message type mismatch: {:?}", std::any::type_name::<M>());
            }
        });

        // Use the type ID of the concrete message type M as the key in the handlers map.
        let type_id = TypeId::of::<M>();
        match self.message_reactors.insert(type_id, message_reactor_box){
            None => {
                trace!("Inserted new reactor with no existing reactor");
            }
            Some(_) => {
                trace!("Reactor added to the map, replacing an existing reactor");
            }
        };
        trace!("Reactor map size: {}", self.message_reactors.len());

        // Return self to allow chaining.
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

