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

use std::cmp::PartialEq;
use std::sync::Arc;

use tokio::runtime::Runtime;
use tracing::{error, instrument, trace};

use crate::common::{Envelope, MessageError};
use crate::message::return_address::ReturnAddress;
use crate::traits::ActonMessage;

/// Represents an outbound envelope for sending messages in the actor system.
#[derive(Clone, Debug, Default)]
pub struct OutboundEnvelope {
    pub(crate) return_address: ReturnAddress,
}

impl PartialEq for ReturnAddress {
    fn eq(&self, other: &Self) -> bool {
        self.sender == other.sender
    }
} // Manually implement PartialEq for OutboundEnvelope
impl PartialEq for OutboundEnvelope {
    fn eq(&self, other: &Self) -> bool {
        self.return_address == other.return_address
    }
}

// Implement Eq for OutboundEnvelope as it is required when implementing PartialEq
impl Eq for OutboundEnvelope {}

// Implement Hash for OutboundEnvelope as it is required for HashSet
impl std::hash::Hash for OutboundEnvelope {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.return_address.sender.hash(state);
    }
}

impl OutboundEnvelope {
    /// Creates a new outbound envelope.
    ///
    /// # Parameters
    /// - `reply_to`: The optional channel for sending replies.
    /// - `sender`: The sender's ARN.
    ///
    /// # Returns
    /// A new `OutboundEnvelope` instance.
    #[instrument(skip(return_address))]
    pub fn new(return_address: ReturnAddress) -> Self {
        OutboundEnvelope { return_address }
    }

    /// Sends a reply message synchronously.
    ///
    /// # Parameters
    /// - `message`: The message to be sent.
    /// - `pool_id`: An optional pool ID.
    ///
    /// # Returns
    /// A result indicating success or failure.
    #[instrument(skip(self))]
    pub fn reply(
        &self,
        message: impl ActonMessage + Sync + Send + 'static,
    ) -> Result<(), MessageError> {
        let envelope = self.clone();
        trace!("*");
        // Event: Replying to Message
        // Description: Replying to a message with an optional pool ID.
        // Context: Message details and pool ID.
        let _ = tokio::task::spawn_blocking(move || {
            tracing::trace!(msg = ?message, "Replying to message.");
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                envelope.reply_async(message).await;
            });
        });
        Ok(())
    }

    /// Sends a reply message asynchronously.
    ///
    /// # Parameters
    /// - `message`: The message to be sent.
    /// - `pool_id`: An optional pool ID.
    ///
    /// # Returns
    /// A result indicating success or failure.
    #[instrument(skip(self))]
    async fn reply_message_async(&self, message: Arc<dyn ActonMessage + Send + Sync>) {
        let reply_to = &self.return_address;
        let type_id = (&*message).type_id();
        if !reply_to.address.is_closed() {
            // Reserve capacity
            match reply_to.address.reserve().await {
                Ok(permit) => {
                    let envelope = Envelope::new(message, self.return_address.clone());
                    permit.send(envelope);
                    trace!(
                        "Reply to {} from OutboundEnvelope",
                        &self.return_address.sender
                    )
                }
                Err(_) => {
                    error!(
                        "Failed to reply to {} from OutboundEnvelope with message type {:?}",
                        &self.return_address.sender, &type_id
                    )
                }
            }
        } else {
            error!(
                "reply_message_async to is closed for {} with message {:?}",
                self.return_address.sender, message
            );
        }
    }

    /// Sends a reply message asynchronously.
    ///
    /// # Parameters
    /// - `message`: The message to be sent.
    /// - `pool_id`: An optional pool ID.
    ///
    /// # Returns
    /// A result indicating success or failure.
    #[instrument(skip(self))]
    pub async fn reply_async(&self, message: impl ActonMessage + Sync + Send + 'static) {
        self.reply_message_async(Arc::new(message)).await;
    }

    /// Sends a reply message asynchronously.
    ///
    /// # Parameters
    /// - `message`: The message to be sent.
    /// - `pool_id`: An optional pool ID.
    ///
    /// # Returns
    /// A result indicating success or failure.
    #[instrument(skip(self))]
    pub async fn reply_async_boxed(
        &self,
        message: Arc<dyn ActonMessage + Send + Sync>,
        pool_id: Option<String>,
    ) {
        self.reply_message_async(message).await;
    }
}
