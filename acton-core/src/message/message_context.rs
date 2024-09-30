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
use tracing::instrument;

use crate::message::OutboundEnvelope;

/// Represents a record of an event within the actor system.
///
/// # Type Parameters
/// - `S`: The type of the message contained in the event.
#[derive(Clone, Debug)]
pub struct MessageContext<S> {
    /// The message contained in the event.
    pub(crate) message: S,
    /// The time when the message was sent.
    pub(crate) timestamp: SystemTime,
    /// The return address for the message response.
    pub(crate) origin_envelope: OutboundEnvelope,
    pub(crate) reply_envelope: OutboundEnvelope,
}

impl<S> MessageContext<S>{
    pub fn origin_envelope(&self) -> &OutboundEnvelope {
        &self.origin_envelope
    }
    pub fn reply_envelope(&self) -> &OutboundEnvelope {
        &self.reply_envelope
    }
    pub fn message(&self) -> &S {
        &self.message
    }
    pub fn timestamp(&self) -> &SystemTime {
        &self.timestamp
    }
}

// Ensures that EventRecord<u32> implements the Send trait.
assert_impl_all!(MessageContext<u32>: Send);
