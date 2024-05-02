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
use async_trait::async_trait;
use futures::future;
use quasar_qrn::Qrn;
use tokio::sync::mpsc::channel;
use tokio::sync::Mutex;
use tokio::task;
use tokio_util::task::TaskTracker;
use crate::common::{SystemSignal, Context, Idle, Awake, OutboundChannel, OutboundEnvelope, StopSignal};
use tracing::{debug, error, instrument, trace};
use crate::traits::{QuasarMessage, SystemMessage};

pub struct Actor<RefType: Send + 'static, State: Default + Send + Debug + 'static> {
    pub ctx: RefType,
    pub outbox: Option<OutboundChannel>,
    pub(crate) parent_return_envelope: Option<OutboundEnvelope>,
    pub halt_signal: StopSignal,
    pub key: Qrn,
    pub state: State,
}

unsafe impl<RefType: Send + 'static, State: Default + Send + Debug + 'static> Send for Actor<RefType, State> {}

impl<State: Default + Send + Debug + 'static> Actor<Awake<State>, State> {
    pub fn new_envelope(&mut self) -> Option<OutboundEnvelope> {
        if let Some(envelope) = &self.outbox {
            Option::from(OutboundEnvelope::new(Some(envelope.clone()), self.key.clone()))
        } else { None }
    }
}

impl<State: Default + Send + Debug + 'static> Actor<Idle<State>, State> {
    pub(crate) fn new(key: Qrn, state: State, parent_return_envelope: Option<OutboundEnvelope>) -> Self {
        Actor {
            ctx: Idle::new(),
            outbox: None,
            parent_return_envelope,
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
        // if active_actor.parent_return_envelope.clone().is_none() {
        //     tracing::error!("{}", active_actor.key.value);
        // } else {
        //     if let Some(env) = &active_actor.parent_return_envelope {
        //         tracing::error!("sender: {}", env.sender.value);
        //     }
        // }
        // debug_assert!(&active_actor.parent_return_envelope.clone().is_some(), "Task tracker must be closed after operations");

        let mut actor = active_actor;

        let qrn = actor.key.clone();

        let task_tracker = TaskTracker::new();
        let (outbox, mailbox) = channel(255);
        // actor.parent_return_envelope = Some(OutboundEnvelope::new(Some(outbox.clone())));
        actor.outbox = Some(outbox.clone());


        // Run the local task set.
        task_tracker.spawn(async move {
            Awake::wake(mailbox, actor, reactors).await
        });

        let outbox = Some(outbox);
        task_tracker.close();
        debug_assert!(task_tracker.is_closed(), "Task tracker must be closed after operations");
        Context {
            outbox,
            task_tracker,
            key: qrn,
            pools: Default::default(),
            //TODO: Revisit this is a bad solution. Currently used for a rudimentary round-robin distribution
            current_index: 0,
        }
    }
}

impl<State: Default + Send + Debug + 'static> Actor<Awake<State>, State> {
    #[instrument(skip(self))]
    pub(crate) fn terminate(&self) {
        if !self.halt_signal.load(Ordering::SeqCst) {
            trace!("{:?}", self.halt_signal);
            self.halt_signal.store(true, Ordering::SeqCst);
        }
    }
}

