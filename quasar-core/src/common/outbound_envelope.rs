/*
 *
 *  * Copyright (c) 2024 Govcraft.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

use quasar_qrn::Qrn;
use tokio::task::block_in_place;
use tracing::instrument;

use crate::common::{Envelope, MessageError, OutboundChannel};
use crate::prelude::QuasarMessage;

#[derive(Clone, Debug, Default)]
pub struct OutboundEnvelope {
    pub sender: Qrn,
    pub(crate) reply_to: Option<OutboundChannel>,
}

impl OutboundEnvelope {
    #[instrument(skip(reply_to))]
    pub fn new(reply_to: Option<OutboundChannel>, sender: Qrn) -> Self {
        OutboundEnvelope { reply_to, sender }
    }
    #[instrument(skip(self, message, pool_id), fields(sender=self.sender.value))]
    pub fn reply(
        &self,
        message: impl QuasarMessage + Send + Sync + 'static,
        pool_id: Option<String>,
    ) -> Result<(), MessageError> {
        block_in_place(|| {
            let future = self.reply_async(message, pool_id);
            tokio::runtime::Handle::current().block_on(future)
        })
    }
    #[instrument(skip(self, message, pool_id), fields(sender=self.sender.value))]
    pub async fn reply_async(
        &self,
        message: impl QuasarMessage + Send + Sync + 'static,
        pool_id: Option<String>,
    ) -> Result<(), MessageError> {
        //        tracing::trace!("{}", self.sender.value);

        if let Some(reply_to) = &self.reply_to {
            debug_assert!(!reply_to.is_closed(), "reply_to was closed in reply");
            let envelope = Envelope::new(Box::new(message), self.reply_to.clone(), pool_id);
            reply_to.send(envelope).await?;
        }
        Ok(())
    }
    #[instrument(skip(self, message),fields(sender=self.sender.value))]
    pub(crate) async fn reply_all(
        &self,
        message: impl QuasarMessage + Send + Sync + 'static,
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
