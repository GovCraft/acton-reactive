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
use std::sync::{Arc};
use std::sync::atomic::Ordering;
use std::time::Duration;
use quasar_qrn::Qrn;
use tokio::sync::mpsc::channel;
use tokio::sync::Mutex;
use tracing::{debug, instrument, trace};
use crate::common::{InboundChannel, OutboundChannel, MessageReactorMap, StopSignal, LifecycleReactor, InboundSignalChannel, OutboundSignalChannel, SignalReactorMap, Idle};
use crate::common::*;

pub struct Awake<T: 'static, U: 'static> {
    pub key: Qrn,
    pub state: T,
    pub(crate) signal_reactors: Option<SignalReactorMap<T, U>>,
    signal_mailbox: InboundSignalChannel,
    pub(crate) signal_outbox: OutboundSignalChannel,
    on_wake: LifecycleReactor<Awake<T, U>>,
    on_stop: LifecycleReactor<Awake<T, U>>,
    pub(crate) message_reactors: Option<MessageReactorMap<T, U>>,
    mailbox: InboundChannel,
    pub(crate) outbox: OutboundChannel,
    halt_signal: StopSignal,
}

impl<T, U> Awake<T, U> {
    #[instrument(skip(actor, message_reactors, signal_reactors), fields(key))]
    pub(crate) async fn wake(actor: Arc<Mutex<Awake<T, U>>>, message_reactors: MessageReactorMap<T, U>, signal_reactors: SignalReactorMap<T, U>) {
        // Pre-fetch key value outside of the loop to avoid locking just for reading the key.
        let key = actor.lock().await.key.value.clone();
        let actor = actor.clone();  // Clone outside the loop to reduce overhead.

        loop {
            // Reduce the scope of the lock to just the operations that need it.
            let envelope = {
                let cloned_actor = actor.clone();  // Clone the actor for use in this iteration.
                let mut actor_guard = cloned_actor.lock().await;
                actor_guard.mailbox.try_recv()  // Try receiving a message.
            };

            if let Ok(envelope) = envelope {
                trace!("Received actor message: {:?}", envelope);
                let type_id = envelope.message.as_any().type_id();

                if let Some(reactor) = message_reactors.get(&type_id) {
                    let actor_arc = actor.clone();  // Clone the actor for reactor use.
                    let _ = (*reactor)(actor_arc, &envelope).await;
                } else {
                    trace!("No reactor for message type: {:?}", type_id);
                }
            }

            // Handling internal signals with minimized lock duration.
            let internal_signal = {
                let cloned_actor = actor.clone();
                let x = cloned_actor.lock().await.signal_mailbox.try_recv();
                x
            };

            if let Ok(internal_signal) = internal_signal {
                trace!("received internal signal");
                let type_id = internal_signal.as_any().type_id();
                if let Some(reactor) = signal_reactors.get(&type_id) {
                    trace!("received stop signal");
                    let cloned_actor = actor.clone();
                    (*reactor)(cloned_actor, &*internal_signal).await;
                } else {
                    trace!("No handler for message type: {:?}", internal_signal);
                    continue;
                }
            } else {
                // Simulate delay to avoid tight loop overwhelming.
                tokio::time::sleep(Duration::from_nanos(1)).await;
            }

            // Checking stop condition with minimized lock duration.
            let should_stop = {
                let cloned_actor = actor.clone();
                let actor_guard = cloned_actor.lock().await;
                actor_guard.halt_signal.load(Ordering::SeqCst) && actor_guard.mailbox.is_empty()
            };

            if should_stop {
                debug!("Halt signal received, exiting capture loop");
                break;
            }
        }

// This comment block seems to indicate planned future changes or considerations for stopping the actor.
// Please ensure to handle this appropriately when you finalize your logic.
        // (actor.lock().await.on_stop)(actor);
    }
    #[instrument(skip(self))]
    pub(crate) fn terminate(&self) {
        debug!("Inside terminate");
        if !self.halt_signal.load(Ordering::SeqCst) {
            debug!("Halt signal loaded");
            self.halt_signal.store(true, Ordering::SeqCst);
        }
    }
}


impl<T: Default + Send + Sync, U: Send + Sync> From<Actor<Idle<T, U>>> for Actor<Awake<T, U>> {
    #[instrument("from idle to awake", skip(value))]
    fn from(value: Actor<Idle<T, U>>) -> Actor<Awake<T, U>> {
        let (outbox, mailbox) = channel(255);
        let (signal_outbox, signal_mailbox) = channel(255);
        let on_wake = value.state.on_wake;
        let on_stop = value.state.on_stop;
        let message_reactors = Some(value.state.message_reactors);
        let signal_reactors = Some(value.state.signal_reactors);
        let halt_signal = StopSignal::new(false);

        Actor {
            state: Awake {
                signal_outbox,
                signal_mailbox,
                on_wake,
                on_stop,
                message_reactors,
                signal_reactors,
                mailbox,
                outbox,
                halt_signal,
                key: value.state.key,
                state: value.state.state,
            },
        }
    }
}
