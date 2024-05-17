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

use crate::common::OutboundEnvelope;
use static_assertions::assert_impl_all;
use std::time::SystemTime;

#[derive(Clone, Debug)]
pub struct EventRecord<S> {
    pub message: S,
    pub sent_time: SystemTime,
    pub return_address: OutboundEnvelope,
}
assert_impl_all!(EventRecord<u32>: Send);
