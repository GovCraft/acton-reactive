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


pub struct Awake<State: Default + Send + Sync + Debug + 'static> {
    on_wake: Box<LifecycleReactor<Awake<State>, State>>,
    pub(crate) on_stop: Box<LifecycleReactor<Awake<State>, State>>,
}

impl<State: Default + Send + Sync + Debug + 'static> Awake<State> {
    #[instrument(skip(actor, mailbox, reactors))]
    pub(crate) async fn wake(mut mailbox: InboundChannel, mut actor: Actor<Awake<State>, State>, reactors: ReactorMap<State>) {
        (actor.actor_ref.on_wake)(&actor);

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
            // Checking stop condition .
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

        (actor.actor_ref.on_stop)(&actor);
    }
}


impl<State: Default + Send + Sync + Debug + 'static> From<Actor<Idle<State>, State>> for Actor<Awake<State>, State> {
    #[instrument("from idle to awake", skip(value))]
    fn from(value: Actor<Idle<State>, State>) -> Actor<Awake<State>, State> {
        let on_wake = value.actor_ref.on_wake;
        let on_stop = Box::new(value.actor_ref.on_stop);
        let halt_signal = StopSignal::new(false);

        Actor {
            actor_ref: Awake {
                on_wake,
                on_stop,
            },
            outbox: None,
            halt_signal: Default::default(),
            key: value.key,
            state: value.state,
        }
    }
}
