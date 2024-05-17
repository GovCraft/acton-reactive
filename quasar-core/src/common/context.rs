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

use crate::common::{Actor, Idle, OutboundChannel, OutboundEnvelope, SystemSignal};
use crate::traits::{ActorContext, QuasarMessage, SupervisorContext};
use async_trait::async_trait;
use quasar_qrn::Qrn;
use std::fmt::Debug;
use tokio_util::task::TaskTracker;
use tracing::instrument;

#[derive(Debug, Clone, Default)]
pub struct Context {
    pub key: Qrn,
    pub(crate) outbox: Option<OutboundChannel>,
    pub(crate) supervisor_task_tracker: TaskTracker,
    pub(crate) task_tracker: TaskTracker,
    pub(crate) supervisor_outbox: Option<OutboundChannel>,
}

impl Context {
    #[instrument(skip(self))]
    pub fn new_actor<State: Default + Send + Debug>(&self, id: &str) -> Actor<Idle<State>, State> {
        tracing::trace!("Creating new actor with id: {}", id);
        let parent_context = self.clone();

        let actor = Actor::new(id, State::default(), Some(parent_context));

        // Check if the mailbox is closed
        debug_assert!(
            !actor.mailbox.is_closed(),
            "Actor mailbox is closed in new_actor"
        );

        tracing::trace!("New actor created with key: {}", actor.key.value);

        actor
    }
    #[instrument]
    pub async fn emit_pool(&self, name: &str, message: impl QuasarMessage + Sync + Send + 'static) {
        self.pool_emit(name, message).await.expect("");
    }

    #[instrument(skip(self))]
    pub async fn terminate(&self) -> anyhow::Result<()> {
        let supervisor_tracker = self.supervisor_task_tracker().clone();
        let tracker = self.get_task_tracker().clone();
        self.terminate_subordinates().await?;
        supervisor_tracker.wait().await;
        self.terminate_actor().await?;
        tracker.wait().await;
        Ok(())
    }

    #[instrument]
    pub(crate) async fn terminate_subordinates(&self) -> anyhow::Result<()> {
        tracing::trace!("entering terminate_all");
        let supervisor = self.supervisor_return_address().clone();
        if let Some(supervisor) = supervisor {
            supervisor.reply_all(SystemSignal::Terminate).await?;
        }
        Ok(())
    }

    #[instrument]
    pub(crate) async fn terminate_actor(&self) -> anyhow::Result<()> {
        tracing::trace!("entering terminate_actor");
        let actor = self.return_address().clone();
        actor.reply(SystemSignal::Terminate, None).await?;
        Ok(())
    }
}

#[async_trait]
impl SupervisorContext for Context {
    #[instrument(skip(self))]
    fn supervisor_return_address(&self) -> Option<OutboundEnvelope> {
        if let Some(outbox) = &self.supervisor_outbox {
            let outbox = outbox.clone();
            debug_assert!(
                !outbox.is_closed(),
                "Outbox was closed in supervisor_return_address"
            );
            Some(OutboundEnvelope::new(Some(outbox), self.key.clone()))
        } else {
            None
        }
    }
    fn supervisor_task_tracker(&self) -> TaskTracker {
        self.supervisor_task_tracker.clone()
    }
}

#[async_trait]
impl ActorContext for Context {
    #[instrument(skip(self))]
    fn return_address(&self) -> OutboundEnvelope {
        let outbox = self.outbox.clone();
        //    tracing::trace!("");
        OutboundEnvelope::new(outbox, self.key.clone())
    }

    fn get_task_tracker(&self) -> TaskTracker {
        self.task_tracker.clone()
    }

    fn key(&self) -> &Qrn {
        &self.key
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
