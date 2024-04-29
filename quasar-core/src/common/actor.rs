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

use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use futures::future;
use quasar_qrn::Qrn;
use tokio::sync::mpsc::channel;
use tokio::sync::Mutex;
use tokio_util::task::TaskTracker;
use crate::common::{SystemSignal, Context, Idle, Awake, OutboundChannel, OutboundEnvelope, StopSignal};
use tracing::{debug, error, instrument, trace};
use crate::traits::{QuasarMessage, SystemMessage};

pub struct Actor<S> {
    pub state: S,
    pub outbox: Option<OutboundChannel>,
    pub halt_signal: StopSignal,

}

impl<T: Default + Send + Sync, U: Send + Sync> Actor<Awake<T, U>> {
    pub fn new_envelope(&mut self) -> Option<OutboundEnvelope> {
        if let Some(envelope) = &self.outbox {
            Option::from(OutboundEnvelope::new(envelope.clone()))
        } else { None }
    }
}

impl<T: Default + Send + Sync, U: Send + Sync> Actor<Idle<T, U>> {
    pub(crate) fn new(qrn: Qrn, state: T) -> Self {
        Actor {
            state: Idle::new(qrn, state),
            outbox: None,
            halt_signal: Default::default(),
        }
    }

    #[instrument(skip(self))]
    // Modified Rust function to avoid the E0499 error by preventing simultaneous mutable borrows of actor.ctx
    pub async fn spawn(self) -> Context {


        let mut actor = self;
        let reactors = mem::take(&mut actor.state.reactors);


        // Handle any pre_start activities
        (actor.state.on_before_wake)(&actor);

        let active_actor: Actor<Awake<T, U>> = actor.into();

        let mut actor = active_actor;

        let qrn = actor.state.key.clone();

        let task_tracker = TaskTracker::new();
        let (outbox, mailbox) = channel(255);
        actor.outbox = Some(outbox.clone());
        task_tracker.spawn(async move {
            Awake::wake(mailbox, actor, reactors).await
        });


        task_tracker.close();
        debug_assert!(task_tracker.is_closed(), "Task tracker must be closed after operations");
        Context {
            outbox,
            task_tracker,
            key: qrn,
        }
    }
    }

impl<T: Send + Sync, U: Send + Sync> Actor<Awake<T, U>> {
    #[instrument(skip(self))]
    pub(crate) fn terminate(&self) {
        if !self.halt_signal.load(Ordering::SeqCst) {
            trace!("{:?}", self.halt_signal);
            self.halt_signal.store(true, Ordering::SeqCst);
        }
    }

}

