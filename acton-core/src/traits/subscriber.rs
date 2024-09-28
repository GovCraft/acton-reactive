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

use crate::common::BrokerRef;

/// Trait for types that can subscribe to a broker.
///
/// Implementors of this trait can be associated with a broker,
/// allowing them to participate in the publish-subscribe messaging system.
pub trait Subscriber {
    /// Retrieves the broker associated with this subscriber.
    ///
    /// # Returns
    ///
    /// An `Option<BrokerRef>` which is:
    /// - `Some(BrokerRef)` if a broker is associated with this subscriber.
    /// - `None` if no broker is currently associated.
    fn get_broker(&self) -> Option<BrokerRef>;
}
