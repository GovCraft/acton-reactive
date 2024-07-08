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
