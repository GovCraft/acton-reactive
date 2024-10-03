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
/// Represents errors that can occur when sending messages in the actor system.
#[derive(Debug)]
pub enum MessageError {
    /// Indicates that sending a message failed.
    SendFailed(String),
    /// Represents other types of errors.
    OtherError(String),
}

impl std::fmt::Display for MessageError {
    /// Formats the `MessageError` for display.
    ///
    /// # Parameters
    /// - `f`: The formatter used for writing formatted output.
    ///
    /// # Returns
    /// A result indicating whether the formatting was successful.
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            MessageError::SendFailed(msg) => write!(f, "Failed to send message: {}", msg),
            MessageError::OtherError(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for MessageError {}

/// Converts a `SendError` from Tokio's MPSC channel to a `MessageError`.
impl<T> From<tokio::sync::mpsc::error::SendError<T>> for MessageError {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        MessageError::SendFailed("Channel closed".into())
    }
}
