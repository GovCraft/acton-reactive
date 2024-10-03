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

use tracing::*;

use crate::message::BrokerRequest;
use crate::traits::ActonMessage;

/// Represents an envelope that carries a message within the actor system.
///
/// This struct encapsulates a message as an `Arc<dyn ActonMessage>`, allowing for
/// efficient and thread-safe sharing of the message across the system.
#[derive(Debug, Clone)]
pub struct BrokerRequestEnvelope {
    /// The actual message being carried, wrapped in an Arc for thread-safe sharing.
    pub message: Arc<dyn ActonMessage + Send + Sync + 'static>,
}

impl From<BrokerRequest> for BrokerRequestEnvelope {
    /// Converts a `BrokerRequest` into a `BrokerRequestEnvelope`.
    ///
    /// This implementation allows for seamless conversion from a `BrokerRequest`
    /// to a `BrokerRequestEnvelope`, extracting the message from the request.
    ///
    /// # Arguments
    ///
    /// * `value` - The `BrokerRequest` to convert.
    ///
    /// # Returns
    ///
    /// A new `BrokerRequestEnvelope` containing the message from the `BrokerRequest`.
    fn from(value: BrokerRequest) -> Self {
        trace!("{:?}", value);
        Self {
            message: value.message,
        }
    }
}

impl BrokerRequestEnvelope {
    /// Creates a new `BrokerRequestEnvelope` instance.
    ///
    /// This method takes a message implementing the `ActonMessage` trait and wraps it
    /// in a `BrokerRequestEnvelope`.
    ///
    /// # Type Parameters
    ///
    /// * `M`: The type of the message, which must implement `ActonMessage + Send + Sync + 'static`.
    ///
    /// # Arguments
    ///
    /// * `request`: The message to be encapsulated in the `BrokerRequestEnvelope`.
    ///
    /// # Returns
    ///
    /// A new `BrokerRequestEnvelope` instance containing the provided message.
    pub fn new<M: ActonMessage + Send + Sync + 'static>(request: M) -> Self {
        let message = Arc::new(request);
        Self { message }
    }
}
