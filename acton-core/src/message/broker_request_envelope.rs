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

use std::any::{Any, TypeId};
use std::sync::Arc;
use std::time::SystemTime;

use static_assertions::assert_impl_all;
use tracing::*;

use crate::common::Outbox;
use crate::message::BrokerRequest;
use crate::traits::ActonMessage;

/// Represents an envelope that carries a message within the actor system.
#[derive(Debug, Clone)]
pub struct BrokerRequestEnvelope {
    pub message: Arc<dyn ActonMessage + Send + Sync + 'static>,
}

impl From<BrokerRequest> for BrokerRequestEnvelope {
    fn from(value: BrokerRequest) -> Self {
        debug!("{:?}", value);
        Self {
            message: value.message,
        }
    }
}

impl BrokerRequestEnvelope {
    pub fn new<M: ActonMessage + Send + Sync + 'static>(request: M) -> Self {
        let message = Arc::new(request);
        Self { message }
    }
}
