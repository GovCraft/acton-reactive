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
use tracing::{debug, error, instrument, trace};

use crate::common::{Envelope, MessageError};
use crate::message::message_address::MessageAddress;
use crate::traits::ActonMessage;

/// Represents an outbound envelope for sending messages in the actor system.
#[derive(Clone, Debug, Default)]
pub struct OutboundEnvelope {
    pub(crate) return_address: MessageAddress,
    pub(crate) recipient_address: Option<MessageAddress>,
}

impl PartialEq for MessageAddress {
    fn eq(&self, other: &Self) -> bool {
        self.sender == other.sender
    }
}

// Manually implement PartialEq for OutboundEnvelope
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
    pub fn new(return_address: MessageAddress) -> Self {
        OutboundEnvelope { return_address, recipient_address: None }
    }

    /// Gets the return address for the outbound envelope.
    pub fn reply_to(&self) -> MessageAddress {
        self.return_address.clone()
    }

    /// Gets the recipient address for the outbound envelope.
    pub fn recipient(&self) -> &Option<MessageAddress> {
        &self.recipient_address
    }

    #[instrument(skip(return_address))]
    pub(crate) fn new_with_recipient(return_address: MessageAddress, recipient_address: MessageAddress) -> Self {
        OutboundEnvelope { return_address, recipient_address: Some(recipient_address) }
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
        message: impl ActonMessage + 'static,
    ) -> Result<(), MessageError> {
        let envelope = self.clone();
        trace!("*");
        // Event: Replying to Message
        // Description: Replying to a message with an optional pool ID.
        // Context: Message details and pool ID.
        tokio::task::spawn_blocking(move || {
            tracing::trace!(msg = ?message, "Replying to message.");
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                envelope.send(message).await;
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
    #[instrument(skip(self), level = "debug")]
    async fn send_message_inner(&self, message: Arc<dyn ActonMessage + Send + Sync>) {
        let recipient_channel = {
            if let Some(recipient_address) = &self.recipient_address {
                recipient_address.clone()
            } else {
                self.return_address.clone()
            }
        };
        let recipient_id = &recipient_channel.sender.root.to_string();
        let address = &recipient_channel.address;

        if !&address.is_closed() {
            // Reserve capacity
            match recipient_channel.clone().address.reserve().await {
                Ok(permit) => {
                    trace!(
                        "...to {} with message: ",
                        recipient_id
                    );
                    let envelope = Envelope::new(message, self.return_address.clone(), recipient_channel);
                    permit.send(envelope);
                }
                Err(e) => {
                    error!(
                        "{}::{}",
                        &self.return_address.name(), e.to_string()
                    )
                }
            }
        } else {
            error!(
    "recipient channel closed: {}",
    self.return_address.name()
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
    #[instrument(skip(self), level = "trace")]
    pub async fn send(&self, message: impl ActonMessage + 'static) {
        self.send_message_inner(Arc::new(message)).await;
    }
}
