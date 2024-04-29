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
use std::sync::atomic::Ordering;
use std::time::Duration;
use dashmap::mapref::one::Ref;

use quasar_qrn::Qrn;
use tokio::sync::mpsc::channel;

use tracing::{debug, instrument, trace, warn};
use tracing::field::debug;
use crate::common::{InboundChannel, MessageReactorMap, StopSignal, LifecycleReactor, InboundSignalChannel, OutboundSignalChannel, SignalReactorMap, Idle};
use crate::common::*;
use crate::traits::QuasarMessage;
// use crate::prelude::QuasarMessage;


pub struct Awake<T: Send + Sync + 'static, U: Send + Sync + 'static> {
    pub key: Qrn,
    pub state: T,
    pub(crate) signal_reactors: Option<SignalReactorMap<T, U>>,
    signal_mailbox: InboundSignalChannel,
    pub(crate) signal_outbox: OutboundSignalChannel,
    on_wake: Box<LifecycleReactor<Awake<T, U>>>,
    pub(crate) on_stop: Box<LifecycleReactor<Awake<T, U>>>,
}

impl<T: Send + Sync + 'static, U: Send + Sync + 'static> Awake<T, U> {
    #[instrument(skip(actor, mailbox, reactors))]
    pub(crate) async fn wake(mut mailbox: InboundChannel, mut actor: Actor<Awake<T, U>>, reactors: ReactorMap<T, U>) {
        (actor.state.on_wake)(&actor);
        // let actor = actor.clone();  // Clone outside the loop to reduce overhead.
        loop {
            if let Ok(envelope) = mailbox.try_recv() {
                trace!("Received actor message: {:?}", envelope);
                let type_id = envelope.message.as_any().type_id();

                if let Some(reactor) = reactors.get(&type_id) {
                    match reactor.value() {
                        ReactorItem::Message(reactor) => {
                            trace!("Executing reactor message");
                            let _ = (*reactor)(&mut actor, &envelope);
                        }
                        ReactorItem::Future(fut) => {
                            trace!("Executing reactor future");
                            let mut actor = &mut actor;
                            (*fut)(&mut actor, &envelope).await;
                        }
                        _ => {}
                    }
                } else {
                    if let Some(concrete_msg) = envelope.message.as_any().downcast_ref::<SystemSignal>() {
                        trace!("{:?}", concrete_msg);
                        match concrete_msg {
                            SystemSignal::Wake => {}
                            SystemSignal::Recreate => {}
                            SystemSignal::Suspend => {}
                            SystemSignal::Resume => {}
                            SystemSignal::Terminate => {
                                actor.terminate();
                            }
                            SystemSignal::Supervise => {}
                            SystemSignal::Watch => {}
                            SystemSignal::Unwatch => {}
                            SystemSignal::Failed => {}
                        }
                    } else {
                        // Return an immediately resolving future if downcast fails
                        warn!("No reactor for message type: {:?}", type_id);
                    }
                }
            }
            // Checking stop condition with minimized lock duration.
            let should_stop = {
                actor.halt_signal.load(Ordering::SeqCst) && mailbox.is_empty()
            };

            if should_stop {
                trace!("Halt signal received, exiting capture loop");
                break;
            } else {
                tokio::time::sleep(Duration::from_nanos(1)).await;
            }
        }

        (actor.state.on_stop)(&actor);
    }
}


impl<T: Default + Send + Sync, U: Send + Sync> From<Actor<Idle<T, U>>> for Actor<Awake<T, U>> {
    #[instrument("from idle to awake", skip(value))]
    fn from(value: Actor<Idle<T, U>>) -> Actor<Awake<T, U>> {
        let (signal_outbox, signal_mailbox) = channel(255);
        let on_wake = value.state.on_wake;
        let on_stop = Box::new(value.state.on_stop);
        // let message_reactors = Some(value.state.message_reactors);
        let signal_reactors = Some(value.state.signal_reactors);
        let halt_signal = StopSignal::new(false);

        Actor {
            state: Awake {
                signal_outbox,
                signal_mailbox,
                on_wake,
                on_stop,
                // message_reactors,
                signal_reactors,
                // mailbox,
                key: value.state.key,
                state: value.state.state,
            },
            outbox: None,
            halt_signal: Default::default(),
        }
    }
}
