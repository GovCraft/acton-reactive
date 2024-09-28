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

/// System-wide signals used to control actor lifecycle events.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SystemSignal {
    // Wake,
    // Recreate,
    // Suspend,
    // Resume,
    /// Signal to terminate the actor.
    Terminate,
    // Supervise,
    // Watch,
    // Unwatch,
    // Failed,
}
