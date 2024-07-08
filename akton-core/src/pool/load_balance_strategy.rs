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

use rand::{thread_rng, Rng};

use crate::common::ActorRef;
use crate::traits::LoadBalancerStrategy;

/// Implements a round-robin load balancing strategy.
#[derive(Debug, Default)]
pub struct RoundRobinStrategy {
    /// The current index in the round-robin sequence.
    current_index: usize,
}

// impl RoundRobinStrategy {
//     /// Creates a new `RoundRobinStrategy` instance.
//     ///
//     /// # Returns
//     /// A new `RoundRobinStrategy` instance.
//     pub fn new() -> Self {
//         RoundRobinStrategy { current_index: 0 }
//     }
// }

impl LoadBalancerStrategy for RoundRobinStrategy {
    /// Selects an item index from the given list using round-robin strategy.
    ///
    /// # Parameters
    /// - `items`: A slice of `Context` items to select from.
    ///
    /// # Returns
    /// An `Option` containing the selected index, or `None` if the list is empty.
    fn select_context(&mut self, items: &[ActorRef]) -> Option<usize> {
        if items.is_empty() {
            None
        } else {
            self.current_index = (self.current_index + 1) % items.len();
            Some(self.current_index)
        }
    }
}

/// Implements a random load balancing strategy.
#[derive(Debug, Default)]
pub struct RandomStrategy;

impl LoadBalancerStrategy for RandomStrategy {
    /// Selects an item index from the given list using random selection.
    ///
    /// # Parameters
    /// - `items`: A slice of `Context` items to select from.
    ///
    /// # Returns
    /// An `Option` containing the selected index, or `None` if the list is empty.
    fn select_context(&mut self, items: &[ActorRef]) -> Option<usize> {
        if items.is_empty() {
            None
        } else {
            let index = thread_rng().gen_range(0..items.len());
            Some(index)
        }
    }
}

/// Defines the various load balancing strategies available.
#[derive(Debug)]
#[non_exhaustive]
pub enum LoadBalanceStrategy {
    /// Uses a round-robin strategy for load balancing.
    RoundRobin,
    /// Uses a random strategy for load balancing.
    Random,
    /// Uses a least-busy strategy for load balancing.
    LeastBusy,
    /// Uses a hash-based strategy for load balancing.
    HashBased,
}
