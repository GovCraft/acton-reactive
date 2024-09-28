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
use std::fmt::Debug;

use acton_ern::{Ern, UnixTime};

use crate::common::ActorRef;

#[derive(Debug, Clone)]
pub(crate) struct SubscribeBroker {
    pub(crate) subscriber_id: Ern<UnixTime>,
    pub(crate) message_type_id: TypeId,
    pub(crate) subscriber_context: ActorRef,
}
// impl ActonMessage for SubscribeBroker {
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
