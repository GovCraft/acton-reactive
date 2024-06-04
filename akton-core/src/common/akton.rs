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

use tracing::instrument;

use crate::common::{Actor, Idle};
use std::fmt::Debug;
/// Represents an actor with a root state.
///
/// # Type Parameters
/// - `State`: The type representing the state of the actor.
#[derive(Debug)]
pub struct Akton<State: Default + Send + Debug> {
    /// The root state of the actor.
    pub root_actor: State,
}

impl<State: Default + Send + Debug> Akton<State> {
    /// Creates a new root actor in the idle state.
    ///
    /// # Returns
    /// A new `Actor` instance in the idle state with the root state.
    #[instrument]
    pub fn create<'a>() -> Actor<Idle<State>, State>
        where
            State: Default + Send + Debug,
    {
        // Creates a new actor with "root" as its identifier and a default state.
        Actor::new("root", State::default(), None)
    }
    #[instrument]
    pub fn create_with_id<'a>(id: &str) -> Actor<Idle<State>, State>
        where
            State: Default + Send + Debug,
    {
        // Creates a new actor with "root" as its identifier and a default state.
        Actor::new(id, State::default(), None)
    }

}

/// Provides a default implementation for the `Akton` struct.
///
/// This implementation creates a new `Akton` instance with the default root state.
impl<State: Default + Send + Debug> Default for Akton<State> {
    /// Creates a new `Akton` instance with the default root state.
    ///
    /// # Returns
    /// A new `Akton` instance.
    fn default() -> Self {
        Akton {
            root_actor: State::default(),
        }
    }
}
