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

use std::time::SystemTime;

use static_assertions::assert_impl_all;

use crate::message::{MessageAddress, OutboundEnvelope};

/// Represents a record of an event within the actor system.
/// This structure maintains the context of a message, including its content,
/// timing information, and routing details for the actor system.
///
/// # Type Parameters
/// - `S`: The type of the message contained in the event.
#[derive(Clone, Debug)]
pub struct MessageContext<S> {
    /// The actual message payload being transmitted
    pub(crate) message: S,
    /// Records when the message was created/sent
    pub(crate) timestamp: SystemTime,
    /// Contains routing information about where the message originated from
    pub(crate) origin_envelope: OutboundEnvelope,
    /// Contains routing information about where replies should be sent
    pub(crate) reply_envelope: OutboundEnvelope,
}

impl<S> MessageContext<S> {
    /// Returns a clone of the original message envelope
    /// This envelope contains the routing information about the message's origin
    pub fn origin_envelope(&self) -> OutboundEnvelope {
        self.origin_envelope.clone()
    }

    /// Returns a clone of the reply envelope
    /// This envelope contains routing information for sending responses
    pub fn reply_envelope(&self) -> OutboundEnvelope {
        self.reply_envelope.clone()
    }

    /// Creates a new envelope for sending messages to a specific recipient
    /// while maintaining the current message context's return address
    pub fn new_envelope(&self, recipient: &MessageAddress) -> OutboundEnvelope {
        OutboundEnvelope::new_with_recipient(
            self.reply_envelope.return_address.clone(),
            recipient.clone(),
        )
    }

    /// Returns a reference to the message payload
    pub fn message(&self) -> &S {
        &self.message
    }

    /// Returns a reference to the message's timestamp
    pub fn timestamp(&self) -> &SystemTime {
        &self.timestamp
    }
}

// This static assertion ensures that MessageContext can be safely sent between threads
// when the generic type parameter is u32. This is important for concurrent processing.
assert_impl_all!(MessageContext<u32>: Send);
