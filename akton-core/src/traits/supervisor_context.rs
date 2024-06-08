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
use std::future::Future;

use async_trait::async_trait;
use tokio_util::task::TaskTracker;
use tracing::{instrument, trace};

use crate::common::{Envelope, MessageError, OutboundEnvelope};
use crate::traits::actor_context::ActorContext;
use crate::traits::akton_message::AktonMessage;

/// Trait for supervisor context, extending `ActorContext` with supervisor-specific methods.
#[async_trait]
pub(crate) trait SupervisorContext: ActorContext {
    /// Returns the supervisor's task tracker.
    fn supervisor_task_tracker(&self) -> TaskTracker;

    /// Returns the supervisor's return address, if available.
    fn supervisor_return_address(&self) -> Option<OutboundEnvelope>;

    /// Emit an envelope to the supervisor.
    #[instrument(skip(self))]
    fn emit_envelope(&self, envelope: Envelope) -> impl Future<Output=Result<(), MessageError>>
        where
            Self: Sync,
    {
        async {
            let forward_address = self.return_address();
            if let Some(reply_to) = forward_address.reply_to {
                // Event: Emitting Envelope
                // Description: Emitting an envelope to the supervisor.
                // Context: Envelope details.
                trace!(envelope = ?envelope, supervisor=self.return_address().sender.value, "Emitting envelope to the supervisor.");
                reply_to.send(envelope).await?;
            }
            Ok(())
        }
    }


    /// Emit a message to a pool using the supervisor's return address.
    #[instrument(skip(self, message))]
    fn emit_to_pool(&self, name: String, message: impl AktonMessage + Send + Sync + 'static)
                    -> impl Future<Output=()> + Send + Sync + '_  where Self: Sync {
        async move {
            if let Some(envelope) = self.supervisor_return_address() {
                // Event: Emitting Message to Pool
                // Description: Emitting a message to a pool using the supervisor's return address.
                // Context: Pool name and message details.
                trace!(sender=?self.supervisor_return_address().unwrap().sender.value,pool_name = ?name, message = ?message, "Emitting message to pool.");
                envelope.reply_async(message, Some(name)).await;
            }
        }
    }
}
