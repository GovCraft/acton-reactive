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

use crate::common::{Context, MessageError, OutboundEnvelope};
use crate::prelude::Envelope;
use async_trait::async_trait;
use akton_arn::prelude::*;
use std::any::{Any, TypeId};
use std::fmt::Debug;
use std::future::Future;
use tokio_util::task::TaskTracker;
use tracing::instrument;

/// Trait defining the strategy for load balancing.
pub(crate) trait LoadBalancerStrategy: Send + Sync + Debug {
    /// Select an item from a list of contexts.
    fn select_item(&mut self, items: &[Context]) -> Option<usize>;
}

/// Trait for Akton messages, providing methods for type erasure.
pub trait AktonMessage: Any + Send + Debug {
    /// Returns a reference to the message as `Any`.
    fn as_any(&self) -> &dyn Any;

    /// Returns the `TypeId` of the message.
    fn type_id(&self) -> TypeId {
        TypeId::of::<Self>()
    }

    /// Returns a mutable reference to the message as `Any`.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Trait for configurable actors, allowing initialization.
#[async_trait]
pub trait ConfigurableActor: Send + Debug {
    /// Initializes the actor with a given name and root context.
    async fn init(&self, name: String, root: &Context) -> Context;
}

/// Trait for supervisor context, extending `ActorContext` with supervisor-specific methods.
#[async_trait]
pub(crate) trait SupervisorContext: ActorContext {
    /// Returns the supervisor's task tracker.
    fn supervisor_task_tracker(&self) -> TaskTracker;

    /// Returns the supervisor's return address, if available.
    fn supervisor_return_address(&self) -> Option<OutboundEnvelope>;

    /// Emit an envelope to the supervisor.
    #[instrument(skip(self))]
    fn emit_envelope(
        &self,
        envelope: Envelope,
    ) -> impl Future<Output = Result<(), MessageError>> + Sync
        where
            Self: Sync,
    {
        async {
            let forward_address = self.return_address();
            if let Some(reply_to) = forward_address.reply_to {
                reply_to.send(envelope).await?;
            }
            Ok(())
        }
    }

    /// Emit a message to a pool using the supervisor's return address.
    fn pool_emit(
        &self,
        name: &str,
        message: impl AktonMessage + Sync + Send + 'static,
    ) -> impl Future<Output = Result<(), MessageError>> + Sync
        where
            Self: Sync,
    {
        async {
            if let Some(envelope) = self.supervisor_return_address() {
                envelope.reply(message, Some(name.to_string()))?;
            }
            Ok(())
        }
    }
}

/// Trait for actor context, defining common methods for actor management.
#[async_trait]
pub trait ActorContext {
    /// Returns the actor's return address.
    fn return_address(&self) -> OutboundEnvelope;

    /// Returns the actor's task tracker.
    fn get_task_tracker(&self) -> TaskTracker;

    /// Returns the actor's key.
    fn key(&self) -> &Arn;

    /// Emit a message from the actor.
    #[instrument(skip(self))]
    fn emit(
        &self,
        message: impl AktonMessage + Sync + Send + 'static,
    ) -> impl Future<Output = Result<(), MessageError>> + Sync
        where
            Self: Sync,
    {
        async {
            let envelope = self.return_address();
            envelope.reply(message, None)?;
            Ok(())
        }
    }

    /// Wakes the actor.
    async fn wake(&mut self) -> anyhow::Result<()>;

    /// Recreates the actor.
    async fn recreate(&mut self) -> anyhow::Result<()>;

    /// Suspends the actor.
    async fn suspend(&mut self) -> anyhow::Result<()>;

    /// Resumes the actor.
    async fn resume(&mut self) -> anyhow::Result<()>;

    /// Supervises the actor.
    async fn supervise(&mut self) -> anyhow::Result<()>;

    /// Watches the actor.
    async fn watch(&mut self) -> anyhow::Result<()>;

    /// Stops watching the actor.
    async fn unwatch(&mut self) -> anyhow::Result<()>;

    /// Marks the actor as failed.
    async fn failed(&mut self) -> anyhow::Result<()>;
}
