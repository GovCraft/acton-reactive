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

use std::fmt::Debug;
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

pub struct Actor<RefType, State: Default + Send + Sync + Debug + 'static> {
    pub ctx: RefType,
    pub outbox: Option<OutboundChannel>,
    pub halt_signal: StopSignal,
    pub key: Qrn,
    pub state: State

}

impl<State: Default + Send + Sync + Debug + 'static> Actor<Awake<State>, State> {
    pub fn new_envelope(&mut self) -> Option<OutboundEnvelope> {
        if let Some(envelope) = &self.outbox {
            Option::from(OutboundEnvelope::new(envelope.clone()))
        } else { None }
    }
}

impl<State: Default + Send + Sync + Debug + 'static> Actor<Idle<State>, State> {
    pub(crate) fn new(key: Qrn, state: State) -> Self {
        Actor {
            ctx: Idle::new(),
            outbox: None,
            halt_signal: Default::default(),
            key,
            state,
        }
    }

    #[instrument(skip(self))]
    // Modified Rust function to avoid the E0499 error by preventing simultaneous mutable borrows of actor.ctx
    pub async fn spawn(self) -> Context {


        let mut actor = self;
        let reactors = mem::take(&mut actor.ctx.reactors);


        // Handle any pre_start activities
        (actor.ctx.on_before_wake)(&actor);

        let active_actor: Actor<Awake<State>, State> = actor.into();

        let mut actor = active_actor;

        let qrn = actor.key.clone();

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
            pools: Default::default(),
            current_index: 0
        }
    }
    }

impl<State: Default + Send + Sync + Debug + 'static> Actor<Awake<State>, State> {
    #[instrument(skip(self))]
    pub(crate) fn terminate(&self) {
        if !self.halt_signal.load(Ordering::SeqCst) {
            trace!("{:?}", self.halt_signal);
            self.halt_signal.store(true, Ordering::SeqCst);
        }
    }

}

