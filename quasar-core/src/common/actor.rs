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

use crate::common::MessageError;
use crate::common::Supervisor;
use crate::common::{
    Awake, Context, Idle, OutboundChannel, OutboundEnvelope, StopSignal, SystemSignal,
};
use crate::prelude::LoadBalancerStrategy;
use crate::traits::ConfigurableActor;
use crate::traits::{ActorContext, SupervisorContext};
use crate::traits::{QuasarMessage, SystemMessage};
use async_trait::async_trait;
use dashmap::DashMap;
use futures::future;
use quasar_qrn::Qrn;
use std::fmt::Debug;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use tokio::sync::Mutex;
use tokio::task;
use tokio_util::task::{task_tracker, TaskTracker};
use tracing::{debug, error, instrument, trace};

use super::supervisor;
use super::PoolBuilder;
use super::PoolDef;
use super::{Envelope, SupervisorMessage};

pub struct Actor<RefType: Send + 'static, State: Default + Send + Debug + 'static> {
    pub ctx: RefType,
    pub outbox: Option<OutboundChannel>,
    pub(crate) parent_return_envelope: Option<OutboundEnvelope>,
    pub halt_signal: StopSignal,
    pub key: Qrn,
    pub state: State,
    pub(crate) subordinates: DashMap<String, ActorPoolDef>,
    pub(crate) task_tracker: TaskTracker,
}

unsafe impl<RefType: Send + 'static, State: Default + Send + Debug + 'static> Send
    for Actor<RefType, State>
{
}

impl<State: Default + Send + Debug + 'static> Actor<Awake<State>, State> {
    pub fn new_envelope(&self) -> Option<OutboundEnvelope> {
        if let Some(envelope) = &self.outbox {
            Option::from(OutboundEnvelope::new(
                Some(envelope.clone()),
                self.key.clone(),
            ))
        } else {
            None
        }
    }
    pub fn new_parent_envelope(&self) -> Option<OutboundEnvelope> {
        self.parent_return_envelope.clone()
    }
}

impl<State: Default + Send + Debug + 'static> Actor<Idle<State>, State> {
    //    #[instrument(skip(parent_context, state))]
    pub(crate) fn new(id: &str, state: State, parent_context: Option<&Context>) -> Self {
        //tracing::debug!("{:?}", &parent_return_envelope);
        //append to the qrn
        let parent_return_envelope;
        let mut key;
        let mut task_tracker;
        if let Some(parent_context) = &parent_context {
            parent_return_envelope = Some(parent_context.return_address().clone());
            key = parent_context.key().clone();
            key.append_part(id);
            task_tracker = TaskTracker::new();
            tracing::trace!("NEW ACTOR KEY: {}", key.value);
            task_tracker = parent_context.task_tracker.clone();
        } else {
            parent_return_envelope = None;
            key = Qrn::default();
            task_tracker = TaskTracker::new();
            tracing::trace!("NEW ACTOR KEY: {}", key.value);
        }
        Actor {
            ctx: Idle::new(key.clone()),
            outbox: None,
            parent_return_envelope,
            halt_signal: Default::default(),
            key,
            state,
            subordinates: DashMap::new(), //            subordinate: None,
            task_tracker,
        }
    }

    pub async fn define_pool<T: ConfigurableActor + Send + Sync + Default + 'static>(
        &mut self,
        name: &str,
        pool_size: usize,
    ) {
        let pool_item =
            ActorPoolDef::new::<T>(name.to_string(), pool_size, &self.ctx.context).await;
        self.subordinates.insert(name.to_string(), pool_item);
    }
    #[instrument(skip(self))]
    pub async fn spawn(self) -> Context {
        let mut actor = self;
        let reactors = mem::take(&mut actor.ctx.reactors);
        let context = actor.ctx.context.clone();
        let mailbox = actor.ctx.mailbox.take();
        let supervisor_mailbox = actor.ctx.supervisor_mailbox.take();
        debug_assert!(supervisor_mailbox.is_some());
        let subordinates = mem::take(&mut actor.subordinates);
        let active_actor: Actor<Awake<State>, State> = actor.into();
        let mut actor = active_actor;

        //      let supervisor = Supervisor {
        //          key: actor.ctx.key.clone(),
        //          halt_signal: StopSignal::new(false),
        //          subordinates,
        //      };
        let actor_tracker = &context.task_tracker.clone();
        let supervisor_tracker = &context.supervisor_task_tracker.clone();

        if let Some(mailbox) = mailbox {
            actor_tracker.spawn(async move { Awake::wake(mailbox, actor, reactors).await });
        } else {
            tracing::error!("no woke actor");
        }
        //      if let Some(supervisor_mailbox) = supervisor_mailbox {
        //          debug_assert!(!supervisor_mailbox.is_closed());
        //          supervisor_tracker.spawn(async move {
        //              Supervisor::wake_supervisor(supervisor_mailbox, supervisor).await
        //          });
        //      }
        //close the trackers
        context.supervisor_task_tracker().close();
        //        context.task_tracker.close();

        context
    }
    #[instrument(skip(self))]
    pub async fn spawn_with_pools(self, builder: PoolBuilder) -> Context {
        let mut actor = self;

        let reactors = mem::take(&mut actor.ctx.reactors);
        let mut context = actor.ctx.context.clone();

        let mut supervisor = builder.spawn(&context).await;
        let mailbox = actor.ctx.mailbox.take();
        context.supervisor_outbox = Some(supervisor.outbox.clone());
        context.supervisor_task_tracker = supervisor.task_tracker.clone();
        let active_actor: Actor<Awake<State>, State> = actor.into();
        let mut actor = active_actor;

        let actor_tracker = &context.task_tracker.clone();

        if let Some(mailbox) = mailbox {
            actor_tracker.spawn(async move { Awake::wake(mailbox, actor, reactors).await });
        } else {
            tracing::error!("no woke actor");
        }
        debug_assert!(!supervisor.mailbox.is_closed());
        let supervisor_tracker = supervisor.task_tracker.clone();

        supervisor_tracker.spawn(async move { supervisor.wake_supervisor().await });
        //   }
        //close the trackers
        supervisor_tracker.close();
        context.task_tracker.close();

        context
    }
}

impl<State: Default + Send + Debug + 'static> Actor<Awake<State>, State> {
    #[instrument(skip(self))]
    pub(crate) fn terminate(&self) {
        let halt_signal = self.halt_signal.load(Ordering::SeqCst);
        self.halt_signal.store(true, Ordering::SeqCst);
    }

    pub async fn spawn_pool<T: ConfigurableActor + Send + Sync + Default + 'static>(
        &mut self,
        name: &str,
        pool_size: usize,
        parent: &Context,
    ) {
        let pool_item = ActorPoolDef::new::<T>(name.to_string(), pool_size, parent).await;
        self.subordinates.insert(name.to_string(), pool_item);
    }
}

#[derive(Clone)]
pub(crate) struct ActorPoolDef {
    pub(crate) pool: Vec<Context>,
}

impl ActorPoolDef {
    #[instrument(skip(parent))]
    pub(crate) async fn new<T: 'static + crate::traits::ConfigurableActor + Send + Sync>(
        name: String,
        pool_size: usize,
        parent: &Context,
    ) -> ActorPoolDef {
        let mut pool = Vec::new();
        for i in 0..pool_size {
            let name = format!("{}{}", name, i);
            //tracing::debug!("{}", &name);
            //          let context = T::init(name, parent).await;
            //          pool.push(context);
        }

        ActorPoolDef { pool }
    }
}
