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
#![forbid(unsafe_code)]
// #![warn(unused)]

//! Akton Main Library
//!
//! This library provides the main entry point for the Akton actor framework.
//! It includes the prelude module for convenient imports.

/// Prelude module for convenient imports.
///
/// This module re-exports commonly used and required items from the `acton_macro` and `acton_core` crates.
pub mod prelude {
    pub use acton_core::prelude::*;
    pub use acton_macro::*;
}
