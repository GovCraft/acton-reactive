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

use crate::message::OutboundEnvelope;

/// Represents a record of an event within the actor system.
///
/// # Type Parameters
/// - `S`: The type of the message contained in the event.
#[derive(Clone, Debug)]
pub struct EventRecord<S> {
    /// The message contained in the event.
    pub message: S,
    /// The time when the message was sent.
    pub sent_time: SystemTime,
    /// The return address for the message response.
    pub return_address: OutboundEnvelope,
}

// Ensures that EventRecord<u32> implements the Send trait.
assert_impl_all!(EventRecord<u32>: Send);
