/*
 *
 *  *
 *  * Copyright (c) 2024 Govcraft.
 *  *
 *  *  Licensed under the Business Source License, Version 1.1 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *      https://github.com/GovCraft/akton-framework/tree/main/LICENSES
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

use std::any::Any;
use std::sync::Arc;
use akton_arn::Arn;
use tokio::runtime::Runtime;

use tracing::{error, instrument, trace};

use crate::common::{Envelope, MessageError, OutboundChannel};
use crate::traits::AktonMessage;

/// Represents an outbound envelope for sending messages in the actor system.
#[derive(Clone, Debug, Default)]
pub struct OutboundEnvelope {
    /// The sender's ARN (Akton Resource Name).
    pub sender: Arn,
    /// The optional channel for sending replies.
    pub(crate) reply_to: Option<OutboundChannel>,
}

// Manually implement PartialEq for OutboundEnvelope
impl PartialEq for OutboundEnvelope {
    fn eq(&self, other: &Self) -> bool {
        self.sender == other.sender && self.reply_to.is_some() == other.reply_to.is_some()
    }
}

// Implement Eq for OutboundEnvelope as it is required when implementing PartialEq
impl Eq for OutboundEnvelope {}

// Implement Hash for OutboundEnvelope as it is required for HashSet
impl std::hash::Hash for OutboundEnvelope {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.sender.value.hash(state);
        self.reply_to.is_some().hash(state);
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
    #[instrument(skip(reply_to))]
    pub fn new(reply_to: Option<OutboundChannel>, sender: Arn) -> Self {
        OutboundEnvelope { reply_to, sender }
    }

    /// Sends a reply message synchronously.
    ///
    /// # Parameters
    /// - `message`: The message to be sent.
    /// - `pool_id`: An optional pool ID.
    ///
    /// # Returns
    /// A result indicating success or failure.
    #[instrument(skip(self, pool_id), fields(sender = self.sender.value))]
    pub fn reply(
        &self,
        message: impl AktonMessage + Sync + Send + 'static,
        pool_id: Option<String>,
    ) -> Result<(), MessageError> {
        let envelope = self.clone();
trace!("*");
        // Event: Replying to Message
        // Description: Replying to a message with an optional pool ID.
        // Context: Message details and pool ID.
        let _ = tokio::task::spawn_blocking(move || {
            tracing::trace!(msg = ?message, pool_id = ?pool_id, "Replying to message.");
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                envelope.reply_async(message, pool_id).await;
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
    #[instrument(skip(self, pool_id), fields(sender = self.sender.value))]
    async fn reply_message_async(
        &self,
        message: Arc<dyn AktonMessage + Send + Sync>,
        pool_id: Option<String>,
    ) {
        if let Some(reply_to) = &self.reply_to {
            let type_id = (&*message).type_id();
            if !reply_to.is_closed() {
                // Reserve capacity
                match reply_to.reserve().await {
                    Ok(permit) => {
                        let envelope = Envelope::new(message, self.reply_to.clone(), pool_id);
                        permit.send(envelope);
                        trace!("Reply to {} from OutboundEnvelope", &self.sender.value)
                    }
                    Err(_) => {
                        error!(
                        "Failed to reply to {} from OutboundEnvelope with message type {:?}",
                        &self.sender.value,
                        &type_id
                    )
                    }
                }
            } else {
                error!("reply_message_async to is closed for {} with message {:?}", self.sender.value, message);
            }
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
    #[instrument(skip(self, pool_id), fields(sender = self.sender.value))]
    pub async fn reply_async(
        &self,
        message: impl AktonMessage + Sync + Send + 'static,
        pool_id: Option<String>,
    ) {
        self.reply_message_async(Arc::new(message), pool_id).await;
    }

    /// Sends a reply message asynchronously.
    ///
    /// # Parameters
    /// - `message`: The message to be sent.
    /// - `pool_id`: An optional pool ID.
    ///
    /// # Returns
    /// A result indicating success or failure.
    #[instrument(skip(self, pool_id), fields(sender = self.sender.value))]
    pub async fn reply_async_boxed(
        &self,
        message: Arc<dyn AktonMessage + Send + Sync>,
        pool_id: Option<String>,
    ) {
        self.reply_message_async(message, pool_id).await;
    }
}
