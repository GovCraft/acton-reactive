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
use async_trait::async_trait;
use tokio_util::task::TaskTracker;
use tracing::instrument;
use crate::common::*;
use crate::traits::akton_message::AktonMessage;

/// Trait for actor context, defining common methods for actor management.
#[async_trait]
pub trait ActorContext {
    /// Returns the actor's return address.
    fn return_address(&self) -> OutboundEnvelope;

    /// Returns the actor's task tracker.
    fn task_tracker(&self) -> TaskTracker;

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
    async fn fail(&mut self) -> anyhow::Result<()>;
}
