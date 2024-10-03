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

use std::sync::Arc;
use std::time::SystemTime;

use static_assertions::assert_impl_all;

use crate::message::message_address::MessageAddress;
use crate::traits::ActonMessage;

/// Represents an envelope that carries a message within the actor system.
#[derive(Debug, Clone)]
pub struct Envelope {
    /// The message contained in the envelope.
    pub message: Arc<dyn ActonMessage + Send + Sync + 'static>,
    /// The time when the message was sent.
    pub timestamp: SystemTime,
    /// The return address for the message response.
    pub reply_to: MessageAddress,
    pub recipient: MessageAddress,
}

impl Envelope {
    /// Creates a new envelope with the specified message, return address, and pool identifier.
    ///
    /// # Parameters
    /// - `message`: The message to be carried in the envelope.
    /// - `return_address`: The return address for the message response.
    /// - `pool_id`: The identifier of the pool to which this envelope belongs, if any.
    ///
    /// # Returns
    /// A new `Envelope` instance.
    pub fn new(
        message: Arc<dyn ActonMessage + Send + Sync + 'static>,
        reply_to: MessageAddress,
        recipient: MessageAddress,
    ) -> Self {
        let timestamp = SystemTime::now();
        Envelope {
            message,
            recipient,
            reply_to,
            timestamp,
        }
    }
}

// Ensures that Envelope implements the Send trait.
assert_impl_all!(Envelope: Send);
