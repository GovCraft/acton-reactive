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

    /// Returns a map of the actor's children.
    fn children(&self) -> DashMap<String, ActorRef>;

    /// Finds a child actor by its ERN.
    ///
    /// # Arguments
    ///
    /// * `arn` - The ERN of the child actor to find.
    ///
    /// # Returns
    ///
    /// An `Option<ActorRef>` containing the child actor if found, or `None` if not found.
    fn find_child(&self, arn: &Ern<UnixTime>) -> Option<ActorRef>;

    /// Returns the actor's task tracker.
    fn tracker(&self) -> TaskTracker;

    /// Sets the actor's ERN.
    ///
    /// # Arguments
    ///
    /// * `ern` - The new ERN to set for the actor.
    fn set_ern(&mut self, ern: Ern<UnixTime>);

    /// Returns the actor's ERN.
    fn ern(&self) -> Ern<UnixTime>;

    /// Creates a clone of the actor's reference.
    fn clone_ref(&self) -> ActorRef;

    /// Emits a message from the actor, possibly to a pool item.
    ///
    /// # Arguments
    ///
    /// * `message` - The message to emit, implementing `ActonMessage`.
    ///
    /// # Returns
    ///
    /// A `Future` that resolves when the message has been emitted.
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

    /// Sends a message synchronously from the actor.
    ///
    /// # Arguments
    ///
    /// * `message` - The message to be sent, implementing `ActonMessage`.
    ///
    /// # Returns
    ///
    /// A `Result` which is:
    /// - `Ok(())` if the message was successfully sent.
    /// - An error of type `MessageError` if the send operation failed.
    ///
    /// ```
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

    /// Wraps a future in a pinned box.
    ///
    /// This method is useful for converting a future into a pinned boxed future,
    /// which is required returning from async act_on message handlers.
    ///
    /// # Arguments
    ///
    /// * `future` - The future to be wrapped.
    ///
    /// # Returns
    ///
    /// A pinned boxed future.
    /// ```
    fn wrap_future<F>(future: F) -> Pin<Box<F>>
    where
        F: Future<Output=()> + Sized + 'static,
    {
        Box::pin(future)
    }

    /// Creates a no-op (no operation) future.
    ///
    /// This method returns a future that does nothing and completes immediately.
    /// It's useful in situations where you need to provide a future but don't want
    /// it to perform any actual work.
    ///
    /// # Returns
    ///
    /// A pinned boxed future that resolves immediately without doing anything.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use acton_core::prelude::*;
    ///
    /// let no_op_future = Actor::noop();
    /// ```
    fn noop() -> Pin<Box<impl Future<Output=()> + Sized>> {
        Box::pin(async move {})
    }
}
