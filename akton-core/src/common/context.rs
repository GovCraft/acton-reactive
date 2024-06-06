/*
 *
 *  *
 *  * Copyright (c) 2024 Govcraft.
 *  *
 *  *  Licensed under the Business Source License, Version 1.1 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *      https://github.com/GovCraft/akton-framework/tree/main/LICENSES
 *  *
 *  *  Change Date: Three years from the release date of this version of the Licensed Work.
 *  *  Change License: Apache License, Version 2.0
 *  *
 *  *  Usage Limitations:
 *  *    - You may use the Licensed Work for non-production purposes only, such as internal testing, development, and experimentation.
 *  *    - You may not use the Licensed Work for any production or commercial purpose, including, but not limited to, the provision of any service to third parties, without a commercial use license from the Licensor, except as stated in the Exemptions section of the License.
 *  *
 *  *  Exemptions:
 *  *    - Open Source Projects licensed under an OSI-approved open source license.
 *  *    - Non-Profit Organizations using the Licensed Work for non-commercial purposes.
 *  *    - Small For-Profit Companies with annual gross revenues not exceeding $2,000,000 USD.
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

use akton_arn::Arn;
use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::oneshot;
use tokio_util::task::TaskTracker;
use tracing::{event, instrument, Level, span};
use tracing::field::Empty;
use tracing::trace_span;

use crate::common::{Actor, Idle, OutboundChannel, OutboundEnvelope, SystemSignal};
use crate::traits::{ActorContext, AktonMessage, SupervisorContext};

use super::signal::SupervisorSignal;

/// Represents the context in which an actor operates.
#[derive(Debug, Clone, Default)]
pub struct Context {
    /// The unique identifier (ARN) for the context.
    pub key: Arn,
    /// The outbound channel for sending messages.
    pub(crate) outbox: Option<OutboundChannel>,
    /// The task tracker for the supervisor.
    pub(crate) supervisor_task_tracker: TaskTracker,
    /// The task tracker for the actor.
    pub(crate) task_tracker: TaskTracker,
    /// The supervisor's outbound channel for sending messages.
    pub(crate) supervisor_outbox: Option<OutboundChannel>,
    pub(crate) children: DashMap<String, Context>,
}

impl Context {

    #[instrument(skip(self))]
    pub async fn supervise<State: Default + Send + Debug>(
        &self,
        child: Actor<Idle<State>, State>,
    ) -> anyhow::Result<()> {
        let context = child.activate(None).await?;
        let id = context.key.value.clone();
        self.children.insert(id, context);

        Ok(())
    }


    /// Emits a message to a pool.
    ///
    /// # Parameters
    /// - `name`: The name of the pool.
    /// - `message`: The message to be emitted.
    #[instrument]
    pub async fn emit_pool(&self, name: &str, message: impl AktonMessage + Sync + Send + 'static) {
        self.pool_emit(name, message).await;
    }

    /// Terminates the actor and its subordinates.
    ///
    /// # Returns
    /// An `anyhow::Result` indicating success or failure.
    #[instrument(skip(self), fields(children=self.children().len()))]
    pub async fn terminate(&self) -> anyhow::Result<()> {
            event!(Level::TRACE, target_actor=self.key.value);
            let supervisor_tracker = self.supervisor_task_tracker().clone();
            self.terminate_subordinates().await?;
            supervisor_tracker.wait().await;
            self.terminate_actor().await?;
            Ok(())
    }


    /// Terminates all subordinate actors.
    ///
    /// # Returns
    /// An `anyhow::Result` indicating success or failure.
    #[instrument(skip(self), fields(children=self.children().len()))]
    pub(crate) async fn terminate_subordinates(&self) -> anyhow::Result<()> {
        let supervisor = self.supervisor_return_address().clone();
        if let Some(supervisor) = supervisor {
            event!(Level::TRACE, "Terminating supervisor");
            supervisor.reply_all(SystemSignal::Terminate).await?;
        }
        Ok(())
    }

    /// Terminates the actor.
    ///
    /// # Returns
    /// An `anyhow::Result` indicating success or failure.
    #[instrument(skip(self), fields(target_actor=self.key.value, children=self.children().len()))]
    pub(crate) async fn terminate_actor(&self) -> anyhow::Result<()> {
        let actor = self.return_address().clone();
        event!(Level::TRACE, "Sending Terminate to actor");
        actor.reply(SystemSignal::Terminate, None)?;
        let tracker = self.get_task_tracker().clone();
        tracker.wait().await;
        Ok(())
    }

    /// Peeks at the state of the actor.
    ///
    /// # Returns
    /// An `Option` containing the state of the actor if successful, otherwise `None`.
    #[instrument(skip(self), target = "peek_space_span")]
    pub async fn peek_state<State: Default + Send + Debug + Clone + 'static>(&self) -> Option<State> {
        let (sender, receiver) = oneshot::channel();
        let actor = self.return_address().clone();

        if actor.reply(SupervisorSignal::Inspect(Some(sender)), None).is_err() {
            return None;
        }

        match receiver.await {
            Ok(result) => Some(result),
            Err(_) => None,
        }
    }
}

#[async_trait]
impl SupervisorContext for Context {
    /// Returns the task tracker for the supervisor.
    fn supervisor_task_tracker(&self) -> TaskTracker {
        self.supervisor_task_tracker.clone()
    }


    /// Returns the return address for the supervisor, if available.
    #[instrument(skip(self))]
    fn supervisor_return_address(&self) -> Option<OutboundEnvelope> {
        if let Some(outbox) = &self.supervisor_outbox {
            let outbox = outbox.clone();
            if !outbox.is_closed() {
                Some(OutboundEnvelope::new(Some(outbox), self.key.clone()))
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[async_trait]
impl ActorContext for Context {
    /// Returns the return address for the actor.
    #[instrument(skip(self))]
    fn return_address(&self) -> OutboundEnvelope {
        let outbox = self.outbox.clone();
        OutboundEnvelope::new(outbox, self.key.clone())
    }
    // #[instrument(Level::TRACE, skip(self), fields(child_count = self.children.len()))]
    fn children(&self) -> DashMap<String, Context> {
        // event!(Level::TRACE,child_count= self.children.len());
        self.children.clone()
    }

    fn find_child(&self, arn: &str) -> Option<Context> {
        if let Some(item) = self.children.get(arn) {
            Some(item.value().clone())
        } else {
            None
        }
    }

    /// Returns the task tracker for the actor.
    fn get_task_tracker(&self) -> TaskTracker {
        self.task_tracker.clone()
    }

    /// Returns the unique identifier (ARN) for the context.
    fn key(&self) -> &Arn {
        &self.key
    }

    /// Wakes the actor.
    async fn wake(&mut self) -> anyhow::Result<()> {
        unimplemented!()
    }

    /// Recreates the actor.
    async fn recreate(&mut self) -> anyhow::Result<()> {
        unimplemented!()
    }

    /// Suspends the actor.
    async fn suspend(&mut self) -> anyhow::Result<()> {
        unimplemented!()
    }

    /// Resumes the actor.
    async fn resume(&mut self) -> anyhow::Result<()> {
        unimplemented!()
    }

    /// Supervises the actor.
    async fn supervise(&mut self) -> anyhow::Result<()> {
        unimplemented!()
    }

    /// Watches the actor.
    async fn watch(&mut self) -> anyhow::Result<()> {
        unimplemented!()
    }

    /// Stops watching the actor.
    async fn unwatch(&mut self) -> anyhow::Result<()> {
        unimplemented!()
    }

    /// Marks the actor as failed.
    async fn failed(&mut self) -> anyhow::Result<()> {
        unimplemented!()
    }
}
