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

use akton_arn::Arn;
use tokio::task::block_in_place;
use tracing::instrument;

use crate::common::{Envelope, MessageError, OutboundChannel};
use crate::traits::akton_message::AktonMessage;
/// Represents an outbound envelope for sending messages in the actor system.
#[derive(Clone, Debug, Default)]
pub struct OutboundEnvelope {
    /// The sender's ARN (Amazon Resource Name).
    pub sender: Arn,
    /// The optional channel for sending replies.
    pub(crate) reply_to: Option<OutboundChannel>,
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
    #[instrument(skip(self, message, pool_id), fields(sender=self.sender.value))]
    pub fn reply(
        &self,
        message: impl AktonMessage + Send + Sync + 'static,
        pool_id: Option<String>,
    ) -> Result<(), MessageError> {
        block_in_place(|| {
            let future = self.reply_async(message, pool_id);
            tokio::runtime::Handle::current().block_on(future)
        })
    }

    /// Sends a reply message asynchronously.
    ///
    /// # Parameters
    /// - `message`: The message to be sent.
    /// - `pool_id`: An optional pool ID.
    ///
    /// # Returns
    /// A result indicating success or failure.
    #[instrument(skip(self, message, pool_id), fields(sender=self.sender.value))]
    pub async fn reply_async(
        &self,
        message: impl AktonMessage + Send + Sync + 'static,
        pool_id: Option<String>,
    ) -> Result<(), MessageError> {
        if let Some(reply_to) = &self.reply_to {
            let envelope = Envelope::new(Box::new(message), self.reply_to.clone(), pool_id);
            reply_to.send(envelope).await?;
        }
        Ok(())
    }

    /// Sends a reply message to all recipients asynchronously.
    ///
    /// # Parameters
    /// - `message`: The message to be sent.
    ///
    /// # Returns
    /// A result indicating success or failure.
    #[instrument(skip(self, message), fields(sender=self.sender.value))]
    pub(crate) async fn reply_all(
        &self,
        message: impl AktonMessage + Send + Sync + 'static,
    ) -> Result<(), MessageError> {
        if let Some(reply_to) = &self.reply_to {
            debug_assert!(!reply_to.is_closed(), "reply_to was closed in reply_all");
            let envelope = Envelope::new(Box::new(message), self.reply_to.clone(), None);
            reply_to.send(envelope).await?;
            tracing::trace!("reply_all completed");
        }
        Ok(())
    }
}
