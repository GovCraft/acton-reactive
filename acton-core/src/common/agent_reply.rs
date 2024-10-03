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

/// A utility struct for creating act_on response futures.
pub struct AgentReply;

impl AgentReply {
    /// Creates a no-op (no operation) future.
    ///
    /// This method returns a future that does nothing and completes immediately.
    /// It's useful in situations where you need to provide a future but don't want
    /// it to perform any actual work.
    ///
    /// # Returns
    ///
    /// A pinned boxed future that resolves immediately without doing anything.
    /// ```
    pub fn immediate() -> Pin<Box<impl Future<Output=()> + Sized>> {
        Box::pin(async move {})
    }

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
    pub fn from_async<F>(future: F) -> Pin<Box<F>>
    where
        F: Future<Output=()> + Sized,
    {
        Box::pin(future)
    }
}