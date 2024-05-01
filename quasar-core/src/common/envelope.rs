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
use crate::traits::QuasarMessage;
use static_assertions::assert_impl_all;
use crate::common::OutboundChannel;

#[derive(Debug)]
pub struct Envelope {
    pub message: Box<dyn QuasarMessage + Send + Sync + 'static>,
    pub sent_time: SystemTime,
    pub return_address: Option<OutboundChannel>
}

impl Envelope {
    pub fn new(message: Box<dyn QuasarMessage + Sync + Send + 'static>) -> Self {
        Envelope {
            message,
            sent_time: SystemTime::now(),
            return_address:None
        }
    }
}
assert_impl_all!(Envelope: Send);