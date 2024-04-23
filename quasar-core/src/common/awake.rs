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
use std::sync::atomic::Ordering;
use std::time::Duration;
use quasar_qrn::Qrn;
use tokio::sync::mpsc::channel;
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
    #[instrument(skip(self, message_reactors, signal_reactors), fields(qrn = & self.key.value))]
    pub(crate) async fn wake(&mut self, message_reactors: MessageReactorMap<T, U>, signal_reactors: SignalReactorMap<T, U>) {
        (self.on_wake)(self);

        loop {
            trace!("Photon responder map size: {}", message_reactors.len());
            trace!("Singularity signal responder map size: {}", signal_reactors.len());

            // Fetch and process actor messages if available
            while let Ok(envelope) = self.mailbox.try_recv() {
                // trace!("Received actor message: {:?}", envelope.message);
                trace!("Received actor message: {:?}", envelope);
                let type_id = envelope.message.as_any().type_id();

                if let Some(reactor) = message_reactors.get(&type_id) {
                    // Dynamically handle the reactor invocation
                    // todo!()

                    (*reactor)(self, &envelope).await;
                } else {
                    trace!("No reactor for message type: {:?}", type_id);
                }
            }
            // Check lifecycle messages
            if let Ok(internal_signal) = self.signal_mailbox.try_recv() {
                let type_id = internal_signal.as_any().type_id();
                match signal_reactors.get(&type_id) {
                    Some(reactor) => {
                        reactor(self, &*internal_signal);
                    }
                    None => {
                        trace!("No handler for message type: {:?}", internal_signal);
                        continue;
                    }
                }
            } else {
                //give some time back to the tokio runtime
                tokio::time::sleep(Duration::from_nanos(1)).await;
            }

            // Check the stop condition after processing messages
            if self.halt_signal.load(Ordering::SeqCst) && self.mailbox.is_empty() {
                trace!("Halt signal received, exiting capture loop");

                break;
            }
        }
        (self.on_stop)(self);
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
