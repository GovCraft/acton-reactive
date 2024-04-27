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

use std::time::SystemTime;

use crate::common::{Envelope, OutboundChannel};
use crate::prelude::QuasarMessage;


#[derive(Clone, Debug)]
pub struct OutboundEnvelope {
    reply_to: OutboundChannel,
}

impl OutboundEnvelope {
    pub fn new(reply_to: OutboundChannel) -> Self {
        OutboundEnvelope { reply_to }
    }
    pub async fn reply(&self, message: impl QuasarMessage + 'static) -> anyhow::Result<()> {
        let envelope = Envelope { message: Box::new(message), sent_time: SystemTime::now() };
        self.reply_to.send(envelope).await?;

        // self.reply_to.send(envelope).await?;
        Ok(())
    }
}

