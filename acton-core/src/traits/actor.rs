/*
 * Copyright (c) 2024. Govcraft
 *
 * Licensed under either of
 *   * Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *   * MIT license: http://opensource.org/licenses/MIT
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the applicable License for the specific language governing permissions and
 * limitations under that License.
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
    fn children(&self) -> DashMap<String, ActorRef>;
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
    fn send(&self, message: impl ActonMessage + Send + Sync + 'static) -> Result<(), MessageError>
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
