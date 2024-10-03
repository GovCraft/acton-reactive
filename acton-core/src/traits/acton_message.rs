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
use std::any::Any;
use std::fmt::Debug;

use dyn_clone::DynClone;

/// Trait for Acton messages, providing methods for type erasure.
pub trait ActonMessage: DynClone + Any + Send + Sync + Debug {
    /// Returns a reference to the message as `Any`.
    fn as_any(&self) -> &dyn Any;

    /// Returns a mutable reference to the message as `Any`.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<T> ActonMessage for T
where
    T: Any + Send + Sync + Debug + DynClone + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
