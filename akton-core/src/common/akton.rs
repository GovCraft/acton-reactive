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

use std::fmt::Debug;
use std::marker::PhantomData;

use tracing::{info, instrument};

use crate::actors::{Actor, Idle};
use crate::common::Context;

/// Represents an actor with a root state.
///
/// # Type Parameters
/// - `State`: The type representing the state of the actor.
#[derive(Debug)]
pub struct Akton<State: Default + Send + Debug> {
    /// The root state of the actor.
    root_actor: PhantomData<State>,
}

impl<State: Default + Send + Debug> Akton<State> {
    /// Creates a new root actor in the idle state.
    ///
    /// # Returns
    /// A new `Actor` instance in the idle state with the root state.
    #[instrument]
    pub fn create() -> Actor<Idle<State>, State> {
        // Event: Creating Root Actor
        // Description: Creating a new root actor in the idle state.
        // Context: None
        info!("Creating a new root actor in the idle state.");
        Actor::new("root", State::default(), None)
    }

    /// Creates a new actor with the specified identifier.
    ///
    /// # Parameters
    /// - `id`: The identifier for the new actor.
    ///
    /// # Returns
    /// A new `Actor` instance in the idle state with the specified identifier.
    #[instrument]
    pub fn create_with_id(id: &str) -> Actor<Idle<State>, State> {
        // Event: Creating Actor with ID
        // Description: Creating a new actor with the specified identifier.
        // Context: Actor ID.
        info!(
            actor_id = id,
            "Creating a new actor with the specified identifier."
        );
        Actor::new(id, State::default(), None)
    }

    /// Creates a new root actor in the idle state with an optional parent context.
    ///
    /// # Parameters
    /// - `parent_context`: An optional parent context for the new actor.
    ///
    /// # Returns
    /// A new `Actor` instance in the idle state with the root state.
    #[instrument]
    pub fn create_with_context(parent_context: Option<Context>) -> Actor<Idle<State>, State> {
        // Event: Creating Root Actor with Context
        // Description: Creating a new root actor in the idle state with an optional parent context.
        // Context: Parent context details.
        info!(parent_context = ?parent_context, "Creating a new root actor in the idle state with an optional parent context.");
        Actor::new("root", State::default(), parent_context)
    }

    /// Creates a new actor with the specified identifier and an optional parent context.
    ///
    /// # Parameters
    /// - `id`: The identifier for the new actor.
    /// - `parent_context`: An optional parent context for the new actor.
    ///
    /// # Returns
    /// A new `Actor` instance in the idle state with the specified identifier and state.
    #[instrument]
    pub fn create_with_id_and_context(
        id: &str,
        parent_context: Option<Context>,
    ) -> Actor<Idle<State>, State> {
        // Event: Creating Actor with ID and Context
        // Description: Creating a new actor with the specified identifier and an optional parent context.
        // Context: Actor ID and parent context details.
        info!(actor_id = id, parent_context = ?parent_context, "Creating a new actor with the specified identifier and an optional parent context.");
        Actor::new(id, State::default(), parent_context)
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
            root_actor: PhantomData,
        }
    }
}
