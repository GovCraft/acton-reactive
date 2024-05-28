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
use crate::common::{Actor, Idle, OutboundChannel, OutboundEnvelope, SystemSignal};
use crate::traits::{ActorContext, AktonMessage, SupervisorContext};
use async_trait::async_trait;
use akton_arn::Arn;
use std::fmt::Debug;
use tokio::sync::oneshot;
use tokio_util::task::TaskTracker;
use tracing::instrument;

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
}

impl Context {
    /// Creates and supervises a new actor with the given ID and state.
    ///
    /// # Parameters
    /// - `id`: The identifier for the new actor.
    ///
    /// # Returns
    /// A new `Actor` instance in the idle state.
    #[instrument(skip(self))]
    pub fn supervise<State: Clone + Default + Send + Debug>(
        &self,
        id: &str,
    ) -> Actor<Idle<State>, State> {
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

    /// Emits a message to a pool.
    ///
    /// # Parameters
    /// - `name`: The name of the pool.
    /// - `message`: The message to be emitted.
    #[instrument]
    pub async fn emit_pool(&self, name: &str, message: impl AktonMessage + Sync + Send + 'static) {
        self.pool_emit(name, message).await.expect("Failed to emit message to pool");
    }

    /// Terminates the actor and its subordinates.
    ///
    /// # Returns
    /// An `anyhow::Result` indicating success or failure.
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

    /// Terminates all subordinate actors.
    ///
    /// # Returns
    /// An `anyhow::Result` indicating success or failure.
    #[instrument]
    pub(crate) async fn terminate_subordinates(&self) -> anyhow::Result<()> {
        tracing::trace!("Terminating all subordinates");
        let supervisor = self.supervisor_return_address().clone();
        if let Some(supervisor) = supervisor {
            supervisor.reply_all(SystemSignal::Terminate).await?;
        }
        Ok(())
    }

    /// Terminates the actor.
    ///
    /// # Returns
    /// An `anyhow::Result` indicating success or failure.
    #[instrument]
    pub(crate) async fn terminate_actor(&self) -> anyhow::Result<()> {
        tracing::trace!("Terminating actor");
        let actor = self.return_address().clone();
        actor.reply(SystemSignal::Terminate, None)?;
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
            debug_assert!(
                !outbox.is_closed(),
                "Outbox was closed in supervisor_return_address"
            );
            Some(OutboundEnvelope::new(Some(outbox), self.key.clone()))
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
