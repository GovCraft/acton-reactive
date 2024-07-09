/*
 *
 *  *
 *  * Copyright (c) 2024 Govcraft.
 *  *
 *  *  Licensed under the Business Source License, Version 1.1 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *      https://github.com/GovCraft/acton-framework/tree/main/LICENSES
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
use std::pin::Pin;

use acton_ern::{Ern, UnixTime};
use async_trait::async_trait;
use dashmap::DashMap;
use tokio_util::task::TaskTracker;
use tracing::instrument;

use crate::common::*;
use crate::traits::acton_message::ActonMessage;

/// Trait for actor context, defining common methods for actor management.
#[async_trait]
pub trait Actor {
    /// Returns the actor's return address.
    fn return_address(&self) -> OutboundEnvelope;
    fn children(&self) -> DashMap<Ern<UnixTime>, ActorRef>;
    fn find_child(&self, arn: &Ern<UnixTime>) -> Option<ActorRef>;
    /// Returns the actor's task tracker.
    fn tracker(&self) -> TaskTracker;
    fn set_ern(&mut self, ern: Ern<UnixTime>);
    fn ern(&self) -> Ern<UnixTime>;
    fn clone_ref(&self) -> ActorRef;
    /// Emit a message from the actor, possibly to a pool item.
    #[instrument(skip(self), fields(children = self.children().len()))]
    fn emit(
        &self,
        message: impl ActonMessage + Sync + Send,
    ) -> impl Future<Output=()> + Send + Sync + '_
    where
        Self: Sync,
    {
        async move {
            let envelope = self.return_address();
            envelope.reply_async(message).await;
        }
    }

    #[instrument(skip(self))]
    fn send(&self, message: impl ActonMessage + Send + Sync + 'static,
    ) -> Result<(), MessageError>
    where
        Self: Sync,
    {
        let envelope = self.return_address();
        envelope.reply(message)?;
        Ok(())
    }

    /// Suspends the actor.
    fn suspend(&self) -> impl Future<Output=anyhow::Result<()>> + Send + Sync + '_;

    fn wrap_future<F>(future: F) -> Pin<Box<F>>
    where
        F: Future<Output=()> + Sized + 'static,
    {
        Box::pin(future)
    }

    fn noop() -> Pin<Box<impl Future<Output=()> + Sized>> {
        Box::pin(async move {})
    }
}


pub trait FutureWrapper {
    fn wrap<F>(future: F) -> Pin<Box<dyn Future<Output=()> + 'static>>
    where
        F: Future<Output=()> + 'static;

    fn noop() -> Pin<Box<dyn Future<Output=()> + 'static>>;
}


// Blanket implementation for all types that implement ActorContext
impl<T> FutureWrapper for T
where
    T: Actor,
{
    fn wrap<F>(future: F) -> Pin<Box<dyn Future<Output=()> + 'static>>
    where
        F: Future<Output=()> + 'static,
    {
        Box::pin(future)
    }

    fn noop() -> Pin<Box<dyn Future<Output=()> + 'static>> {
        Box::pin(async move {})
    }
}
