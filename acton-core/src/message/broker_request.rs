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

use std::any::TypeId;
use std::sync::Arc;

use tracing::*;

use crate::traits::ActonMessage;

/// Represents a request to the broker for message distribution.
///
/// This struct encapsulates a message along with its type information,
/// allowing the broker to efficiently route messages to appropriate subscribers.
#[derive(Debug, Clone)]
pub struct BrokerRequest {
    /// The actual message being sent, wrapped in an Arc for thread-safe sharing.
    pub message: Arc<dyn ActonMessage + Send + Sync + 'static>,
    /// The name of the message type, useful for debugging and logging.
    pub message_type_name: String,
    /// The TypeId of the message, used for efficient type checking and routing.
    pub message_type_id: TypeId,
}

impl BrokerRequest {
    /// Creates a new `BrokerRequest` instance.
    ///
    /// This method takes a message implementing the `ActonMessage` trait and wraps it
    /// in a `BrokerRequest` along with its type information.
    ///
    /// # Type Parameters
    ///
    /// * `M`: The type of the message, which must implement `ActonMessage + Send + Sync + 'static`.
    ///
    /// # Arguments
    ///
    /// * `message`: The message to be encapsulated in the `BrokerRequest`.
    ///
    /// # Returns
    ///
    /// A new `BrokerRequest` instance containing the provided message and its type information.
    pub fn new<M: ActonMessage + Send + Sync + 'static>(message: M) -> Self {
        let message_type_name = std::any::type_name_of_val(&message).to_string();
        let message_type_id = message.type_id();
        let message = Arc::new(message);
        trace!(
            message_type_name = message_type_name,
            "BroadcastEnvelope::new() message_type_id: {:?}",
            message_type_id
        );
        Self {
            message,
            message_type_id,
            message_type_name,
        }
    }
}
