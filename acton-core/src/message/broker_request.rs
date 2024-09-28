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

/// Represents an envelope that carries a message within the actor system.
#[derive(Debug, Clone)]
pub struct BrokerRequest {
    pub message: Arc<dyn ActonMessage + Send + Sync + 'static>,
    pub message_type_name: String,
    pub message_type_id: TypeId,
}

impl BrokerRequest {
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
