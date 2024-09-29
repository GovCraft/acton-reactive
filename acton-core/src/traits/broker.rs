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

use std::fmt::Debug;
use std::future::Future;

use async_trait::async_trait;
use tracing::instrument;

use crate::common::BrokerRef;
use crate::message::BrokerRequest;
use crate::prelude::ActonMessage;
use crate::traits::{Actor, Subscriber};

/// A broker is a message broker that can broadcast messages to all connected clients.
#[async_trait]
pub trait Broker: Clone + Debug + Default {
    /// Broadcast a message from the broker.
    fn broadcast(&self, message: impl ActonMessage) -> impl Future<Output=()> + Send + Sync + '_;
}



