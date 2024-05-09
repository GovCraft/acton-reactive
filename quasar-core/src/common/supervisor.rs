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

use crate::common::actor::ActorPoolDef;
use crate::common::*;
use crate::common::{Idle, InboundChannel, LifecycleReactor, StopSignal};
use crate::prelude::{ActorContext, SupervisorContext};
use crate::traits::QuasarMessage;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use quasar_qrn::Qrn;
use std::any::TypeId;
use std::env;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::mpsc::channel;
use tokio_util::context;
use tracing::field::debug;
use tracing::{debug, instrument, trace, warn};

pub struct Supervisor {
    pub key: Qrn,
    pub(crate) halt_signal: StopSignal,
    pub(crate) subordinates: DashMap<String, ActorPoolDef>,
}
impl Debug for Supervisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.key.value)
    }
}

impl Supervisor {
    #[instrument(skip(supervisor, mailbox))]
    pub(crate) async fn wake_supervisor(mut mailbox: InboundChannel, supervisor: Supervisor) {
        loop {
            if let Ok(envelope) = mailbox.try_recv() {
                if let Some(ref pool_id) = envelope.pool_id {
                    if let Some(pool_def) = &supervisor.subordinates.get(pool_id) {
                        let context = &pool_def.pool[0];
                        context.emit_envelope(envelope).await;
                    }
                } else {
                    if let Some(concrete_msg) =
                        envelope.message.as_any().downcast_ref::<SystemSignal>()
                    {
                        match concrete_msg {
                            SystemSignal::Wake => {}
                            SystemSignal::Recreate => {}
                            SystemSignal::Suspend => {}
                            SystemSignal::Resume => {}
                            SystemSignal::Terminate => {
                                tracing::trace!("supervisor: {:?}", &supervisor);
                                supervisor.terminate().await;
                            }
                            SystemSignal::Supervise => {}
                            SystemSignal::Watch => {}
                            SystemSignal::Unwatch => {}
                            SystemSignal::Failed => {}
                        }
                    } // Checking stop condition .
                }
            }
            let should_stop =
                { supervisor.halt_signal.load(Ordering::SeqCst) && mailbox.is_empty() };

            if should_stop {
                break;
            } else {
                tokio::time::sleep(Duration::from_nanos(1)).await;
            }
        }
    }
    #[instrument(skip(self))]
    pub(crate) async fn terminate(&self) {
        let subordinates = self.subordinates.clone();
        tracing::trace!("subordinate count: {}", subordinates.len());
        let halt_signal = self.halt_signal.load(Ordering::SeqCst);
        if !halt_signal {
            for item in &subordinates {
                for context in &item.value().pool {
                    let envelope = &context.return_address();
                    //                    tracing::warn!("Terminating {}", &context.key.value);
                    tracing::trace!("Terminating done {:?}", &context);
                    //                        if let Some(envelope) = supervisor {
                    envelope.reply(SystemSignal::Terminate, None);
                    //                       }
                    //context.terminate_subordinates().await;
                    context.terminate_actor().await;
                }
            }
            self.halt_signal.store(true, Ordering::SeqCst);
        }
    }
}
