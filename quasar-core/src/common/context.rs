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
use async_trait::async_trait;
use dashmap::DashMap;
use futures::future::join_all;
use tokio_util::task::TaskTracker;
use crate::common::{OutboundChannel, SystemSignal, OutboundSignalChannel, Actor, Idle, OutboundEnvelope, ActorPool, ContextPool, PoolProxy, NewPoolMessage};
use crate::traits::{ActorContext, ConfigurableActor, InternalSignalEmitter, QuasarMessage};
use quasar_qrn::Qrn;
use tracing::{debug, instrument, trace};
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Debug, Clone)]
pub struct Context
{
    pub(crate) outbox: OutboundChannel,
    pub(crate) task_tracker: TaskTracker,
    pub(crate) key: Qrn,
    pub(crate) pools: ActorPool,
    pub(crate) current_index: usize,
}

impl Context {
    pub fn new_actor<State: Default + Send + Sync + Debug>(&self, id: &str) -> Actor<Idle<State>, State> {
        let actor = Default::default();
        //append to the qrn
        let mut qrn = self.key().clone();
        qrn.append_part(id);

        Actor::new(qrn, actor)
    }
}

#[async_trait]
impl ActorContext for Context {
    fn return_address(&self) -> OutboundEnvelope {
        let outbox = self.outbox.clone();
        OutboundEnvelope::new(outbox)
    }

    fn get_task_tracker(&mut self) -> &mut TaskTracker {
        &mut self.task_tracker
    }


    fn key(&self) -> &Qrn {
        &self.key
    }

    async fn pool_emit<DistributionStrategy>(&mut self, name: &str, message: impl QuasarMessage + Send + 'static) -> anyhow::Result<()> {
        if let Some(item) = self.pools.get(name) {
            let pool = item.value();
            if let Some(context) = pool.iter().nth(self.current_index){
                let envelope = context.return_address();
                envelope.reply(message).await;
                self.current_index += 1;
                if self.current_index >= pool.len() {
                    self.current_index = 0;
                }
            }
        }

        Ok(())
    }

    #[instrument(skip(self), fields(qrn = self.key.value))]
    async fn terminate(&self) -> anyhow::Result<()> {
        // Shutdown managed pools
        let mut tasks = Vec::new();
        for context_pool in &self.pools {
            for (_, context) in context_pool.value().clone() {
                let task = async move {
                    // debug!("Terminating {}", context.key.value);
                    context.terminate().await?;
                    context.task_tracker.wait().await;  // Assuming wait() just waits and does not return a Result
                    Ok::<(), anyhow::Error>(())
                };
                tasks.push(task);
            }
        }
        let _handle = join_all(tasks).await;
        self.emit(SystemSignal::Terminate).await;
        self.task_tracker.wait().await;
        Ok(())
    }
    #[instrument(skip(self))]
    async fn spawn_pool<T: ConfigurableActor + 'static>(&mut self, name: &str, size: usize) -> anyhow::Result<()> {
        let mut tasks = Vec::with_capacity(size);
        for i in 0..size {
            let actor_name = format!("{}{}", name, i);
            let pool_item = T::init(actor_name, self);
            tasks.push(pool_item);
        }
        let contexts = join_all(tasks).await;
        let mut items = DashMap::new();  // DashMap to store the results
        for context in contexts {
            items.insert(context.key.value.clone(), context);  // Collect each result
        }
        self.pools.insert(name.to_string(), items);
        debug!("added pool: {}", name);
        Ok(())
    }


    async fn wake(&mut self) -> anyhow::Result<()> {
        // self.signal_outbox.send(Box::new(SystemSignal::Wake)).await?;
        Ok(())
    }

    async fn recreate(&mut self) -> anyhow::Result<()> {
        // self.signal_outbox.send(Box::new(SystemSignal::Recreate)).await?;
        Ok(())
    }

    async fn suspend(&mut self) -> anyhow::Result<()> {
        // self.signal_outbox.send(Box::new(SystemSignal::Suspend)).await?;
        Ok(())
    }

    async fn resume(&mut self) -> anyhow::Result<()> {
        // self.signal_outbox.send(Box::new(SystemSignal::Resume)).await?;
        Ok(())
    }

    async fn supervise(&mut self) -> anyhow::Result<()> {
        // self.signal_outbox.send(Box::new(SystemSignal::Supervise)).await?;
        Ok(())
    }

    async fn watch(&mut self) -> anyhow::Result<()> {
        // self.signal_outbox.send(Box::new(SystemSignal::Watch)).await?;
        Ok(())
    }

    async fn unwatch(&mut self) -> anyhow::Result<()> {
        // self.signal_outbox.send(Box::new(SystemSignal::Unwatch)).await?;
        Ok(())
    }

    async fn failed(&mut self) -> anyhow::Result<()> {
        // self.signal_outbox.send(Box::new(SystemSignal::Failed)).await?;
        Ok(())
    }
}

// #[async_trait]
// impl InternalSignalEmitter for Context {
//     fn signal_outbox(&mut self) -> &mut OutboundSignalChannel {
//         &mut self.signal_outbox
//     }
// }
