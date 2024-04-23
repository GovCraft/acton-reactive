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
use tracing::{instrument, trace};
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
    // #[instrument(skip(first_shared_self, message_reactors, signal_reactors))]
    pub(crate) async fn wake(first_shared_self: Arc<Mutex<Awake<T, U>>>, message_reactors: MessageReactorMap<T, U>, signal_reactors: SignalReactorMap<T, U>) {
        // (shared_self.lock().await.on_wake)(shared_self);
        // Create a shared Arc<Mutex<>> outside the loop
        // let first_shared_self = Arc::new(Mutex::new(self));
        loop {
            let shared_self = first_shared_self.clone();
            trace!("Photon responder map size: {}", message_reactors.len());
            trace!("Singularity signal responder map size: {}", signal_reactors.len());

            // Fetch and process actor messages if available
            while let Ok(envelope) = shared_self.lock().await.mailbox.try_recv() {
                trace!("Received actor message: {:?}", envelope);
                let type_id = envelope.message.as_any().type_id();

                if let Some(reactor) = message_reactors.get(&type_id) {
                    let actor_arc = shared_self.clone();
                    (*reactor)(actor_arc, &envelope).await;
                } else {
                    trace!("No reactor for message type: {:?}", type_id);
                }
            }
            //TODO: revisit internal signals
            // let shared_self = first_shared_self.clone();
            // // Check lifecycle messages
            // if let Ok(internal_signal) = shared_self.lock().await.signal_mailbox.try_recv() {
            //     let type_id = internal_signal.as_any().type_id();
            //     match signal_reactors.get(&type_id) {
            //         Some(reactor) => {
            //             reactor(shared_self.clone(), &*internal_signal);
            //         }
            //         None => {
            //             trace!("No handler for message type: {:?}", internal_signal);
            //             continue;
            //         }
            //     }
            // } else {
            //     //give some time back to the tokio runtime
            //     tokio::time::sleep(Duration::from_nanos(1)).await;
            // }

            let shared_self = shared_self.clone();
            // Check the stop condition after processing messages
            if shared_self.clone().lock().await.halt_signal.load(Ordering::SeqCst) && shared_self.clone().lock().await.mailbox.is_empty() {
                trace!("Halt signal received, exiting capture loop");

                break;
            }
        }
        // Instead of using `self` directly, use the locked version from `shared_self`
        //TODO: revisit
        // let self_clone = first_shared_self.clone();
        // (self_clone.lock().await.si.unwrap().on_stop)(&*self_clone.lock().await.unwrap());

        // (self.on_stop)(self);
    }

    pub(crate) fn terminate(&self) {
        if !self.halt_signal.load(Ordering::SeqCst) {
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
