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

use std::sync::Arc;
use std::time::SystemTime;
use dyn_clone::DynClone;

use static_assertions::assert_impl_all;

use crate::common::Outbox;
use crate::traits::AktonMessage;
/// Represents an envelope that carries a message within the actor system.
#[derive(Debug, Clone)]
pub struct Envelope {
    /// The message contained in the envelope.
    pub message: Arc<dyn AktonMessage + Send + Sync + 'static>,
    /// The identifier of the pool, if any, to which this envelope belongs.
    pub pool_id: Option<String>,
    /// The time when the message was sent.
    pub sent_time: SystemTime,
    /// The return address for the message response.
    pub return_address: Option<Outbox>,
}

impl Envelope {
    /// Creates a new envelope with the specified message, return address, and pool identifier.
    ///
    /// # Parameters
    /// - `message`: The message to be carried in the envelope.
    /// - `return_address`: The return address for the message response.
    /// - `pool_id`: The identifier of the pool to which this envelope belongs, if any.
    ///
    /// # Returns
    /// A new `Envelope` instance.
    pub fn new(
        message: Arc<dyn AktonMessage + Send + Sync + 'static>,
        return_address: Option<Outbox>,
        pool_id: Option<String>,
    ) -> Self {
        Envelope {
            message,
            sent_time: SystemTime::now(),
            return_address,
            pool_id,
        }
    }
}

// Ensures that Envelope implements the Send trait.
assert_impl_all!(Envelope: Send);
