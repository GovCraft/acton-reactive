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

use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio_util::task::TaskTracker;
use tracing::{event, instrument, Level};

use crate::common::*;
use crate::traits::akton_message::AktonMessage;

/// Trait for actor context, defining common methods for actor management.
#[async_trait]
pub trait ActorContext {
    /// Returns the actor's return address.
    fn return_address(&self) -> OutboundEnvelope;
    fn children(&self) -> DashMap<String, Context>;
    fn find_child(&self, arn: &str) -> Option<Context>;
    /// Returns the actor's task tracker.
    fn task_tracker(&self) -> TaskTracker;
    fn key(&self) -> String;
    fn clone_self(&self) -> Context;
    /// Emit a message from the actor, possibly to a pool item.
    #[instrument(skip(self), fields(children = self.children().len()))]
    fn emit_async(
        &self,
        message: impl AktonMessage + Sync + Send,
        pool_name: Option<&str>,
    ) -> impl Future<Output=()> + Send + Sync + '_
    where
        Self: Sync,
    {
        let pool_name = {
            if let Some(pool_id) = pool_name {
                Some(String::from(pool_id))
            } else {
                None
            }
        };
        async move {
            let envelope = self.return_address();
            event!(Level::TRACE, return_address = envelope.sender.value);
            envelope.reply_async(message, pool_name).await;
        }
    }

    /// Emit a message from the actor.
    #[instrument(skip(self), fields(children = self.children().len()))]
    fn emit_message_async(
        &self,
        message: Arc<dyn AktonMessage + Send + Sync>,
        pool_name: Option<&str>,
    ) -> impl Future<Output=()> + Send + Sync + '_
    where
        Self: Sync,
    {
        let pool_name = {
            if let Some(pool_id) = pool_name {
                Some(String::from(pool_id))
            } else {
                None
            }
        };
        async move {
            let envelope = self.return_address();
            envelope.reply_async_boxed(message, pool_name).await;
        }
    }
    #[instrument(skip(self), fields(self.key.value))]
    fn emit(&self, message: impl AktonMessage + Send + Sync + 'static, pool_name: Option<String>,
    ) -> Result<(), MessageError>
    where
        Self: Sync,
    {
        let envelope = self.return_address();
        event!(Level::TRACE, addressed_to = envelope.sender.value);
        envelope.reply(message, None)?;
        Ok(())
    }

    /// Wakes the actor.
    async fn wake(&mut self) -> anyhow::Result<()>;

    /// Recreates the actor.
    async fn recreate(&mut self) -> anyhow::Result<()>;

    /// Suspends the actor.
    fn suspend(&self) -> impl Future<Output=anyhow::Result<()>> + Send + Sync + '_;

    /// Resumes the actor.
    async fn resume(&mut self) -> anyhow::Result<()>;

    /// Supervises the actor.
    async fn supervise(&mut self) -> anyhow::Result<()>;

    /// Watches the actor.
    async fn watch(&mut self) -> anyhow::Result<()>;

    /// Stops watching the actor.
    async fn unwatch(&mut self) -> anyhow::Result<()>;

    /// Marks the actor as failed.
    async fn fail(&mut self) -> anyhow::Result<()>;
}
