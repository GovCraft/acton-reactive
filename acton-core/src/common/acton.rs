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

use std::thread::sleep;
use crate::common::AgentRuntime;

/// Represents the Acton system.
///
/// The `ActonSystem` struct serves as the central controller for the Acton framework,
/// managing the initialization and coordination of various system components.
/// It provides functionality to launch and prepare the system for operation.
#[derive(Default, Debug, Clone)]
pub struct ActonApp;

impl ActonApp {
    /// Launches the Acton system.
    ///
    /// This method initializes and starts the Acton system, setting up all necessary
    /// components and preparing the system for handling tasks and managing actors.
    ///
    /// # Returns
    ///
    /// A [`AgentRuntime`] instance indicating that the system has been successfully launched
    /// and is ready for operation.
    pub fn launch() -> AgentRuntime {
        let system: ActonApp = Default::default();
        system.into()
    }
}
