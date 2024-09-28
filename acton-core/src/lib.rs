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
// #![warn(missing_docs)]
//#![warn(unused)]
//! Akton Core Library
//!
//! This library provides the core functionality for the Akton actor framework.
//! It includes common utilities, trait definitions, and prelude exports.

/// Common utilities and structures used throughout the Akton framework.
pub(crate) mod common;

pub(crate) mod actor;
pub(crate) mod message;
/// Trait definitions used in the Akton framework.
pub(crate) mod traits;

/// Prelude module for convenient imports.
///
/// This module re-exports commonly used items from the `common` and `traits` modules,
/// as well as the `async_trait` crate.
pub mod prelude {
    pub use acton_ern::*;
    pub use async_trait;

    pub use crate::actor::ActorConfig;
    pub use crate::common::{ActonSystem, ActorRef, Broker, SystemReady};
    pub use crate::message::{BrokerRequest, BrokerRequestEnvelope, OutboundEnvelope};
    pub use crate::traits::{ActonMessage, Actor, Subscribable, Subscriber};
}
