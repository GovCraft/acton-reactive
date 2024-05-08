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
use crate::common::{
    Awake, Context, Idle, OutboundChannel, OutboundEnvelope, StopSignal, SystemSignal,
};
use crate::traits::ActorContext;
use crate::traits::ConfigurableActor;
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

use super::{Envelope, SupervisorMessage};

pub struct Actor<RefType: Send + 'static, State: Default + Send + Debug + 'static> {
    pub ctx: RefType,
    pub outbox: Option<OutboundChannel>,
    pub(crate) parent_return_envelope: Option<OutboundEnvelope>,
    pub halt_signal: StopSignal,
    pub key: Qrn,
    pub state: State,
    pub(crate) subordinates: DashMap<String, ActorPoolDef>,
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
    pub(crate) fn new(
        key: Qrn,
        state: State,
        parent_return_envelope: Option<OutboundEnvelope>,
    ) -> Self {
        Actor {
            ctx: Idle::new(key.clone()),
            outbox: None,
            parent_return_envelope,
            halt_signal: Default::default(),
            key,
            state,
            subordinates: DashMap::new(), //            subordinate: None,
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
        let active_actor: Actor<Awake<State>, State> = actor.into();
        let mut actor = active_actor;

        if let Some(mailbox) = mailbox {
            context
                .task_tracker
                .spawn(async move { Awake::wake(mailbox, actor, reactors).await });
        }
        &context.task_tracker.close();

        context
    }
}

impl<State: Default + Send + Debug + 'static> Actor<Awake<State>, State> {
    #[instrument(skip(self))]
    pub(crate) fn terminate(&self) -> impl Future<Output = Result<(), MessageError>> + Sync {
        let halt_signal = self.halt_signal.load(Ordering::SeqCst).clone();
        let subordinates = self.subordinates.clone();
        let fut = async move {
            if !halt_signal {
                for item in &subordinates {
                    for context in &item.value().pool {
                        context.terminate().await;
                    }
                }
            }
            Ok(())
        };
        self.halt_signal.store(true, Ordering::SeqCst);
        fut
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

    pub(crate) fn pool_emit(&self, message: &SupervisorMessage) -> impl Future<Output = ()> + Sync {
        let envelope = self.new_envelope().clone();
        // TODO: almost there
        // need to send the inner_msg from this function to a pool member by the provided name
        // need to add the name to this supevisor message so we know which to use
        // then we need to abandon the approach below and emit directly from the context of
        // the stored pool item
        //      match message {
        //          SupervisorMessage::PoolEmit(inner_message) => {
        //              if let Some(envelope) = envelope {
        //                  async move { envelope.reply(*message).await }
        //              } else {
        //                  async move { () }
        //              }
        //          }
        //          _ => {
        //              tracing::debug!("didnt' get it");
        //              async move {}
        //          }
        //      };
        async move {}
    }
}
#[derive(Clone)]
pub(crate) struct ActorPoolDef {
    pub(crate) pool: Vec<Context>,
}

impl ActorPoolDef {
    #[instrument(skip(parent))]
    async fn new<T: 'static + crate::traits::ConfigurableActor + Send + Sync>(
        name: String,
        pool_size: usize,
        parent: &Context,
    ) -> ActorPoolDef {
        let mut pool = Vec::new();
        for i in 0..pool_size {
            let name = format!("{}{}", name, i);
            //tracing::debug!("{}", &name);
            let context = T::init(name, parent).await;
            pool.push(context);
        }

        ActorPoolDef { pool }
    }
}
