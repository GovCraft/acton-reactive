/*
 *
 *  * Copyright (c) 2024 Govcraft.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

#[derive(Debug)]
pub enum MessageError {
    SendFailed(String),
    OtherError(String),  // Include other types of errors as needed
}

impl std::fmt::Display for MessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            MessageError::SendFailed(msg) => write!(f, "Failed to send message: {}", msg),
            MessageError::OtherError(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for MessageError {}
impl<T> From<tokio::sync::mpsc::error::SendError<T>> for MessageError {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        MessageError::SendFailed("Channel closed".into())
    }
}
