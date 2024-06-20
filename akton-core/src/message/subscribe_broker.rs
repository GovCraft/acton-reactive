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

use std::any::{Any, TypeId};
use std::fmt::Debug;
use crate::common::Context;
use crate::traits::AktonMessage;

#[derive(Debug,Clone)]
pub(crate) struct SubscribeBroker {
    pub(crate) subscriber_id: String,
    pub(crate) message_type_id: TypeId,
    pub(crate) subscriber_context: Context
}
// impl AktonMessage for SubscribeBroker {
//     /// Returns a reference to the signal as `Any`.
//     fn as_any(&self) -> &dyn Any {
//         self
//     }
//
//     /// Returns a mutable reference to the signal as `Any`.
//     fn as_any_mut(&mut self) -> &mut dyn Any {
//         self
//     }
// }
