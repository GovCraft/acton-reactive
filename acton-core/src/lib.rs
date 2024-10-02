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
#![forbid(missing_docs)]
// #![forbid(unused)]
// #![forbid(dead_code)]
// #![forbid(unused_imports)]
//! Acton Core Library
//!
//! This library provides the core functionality for the Acton actor framework.
//! It includes common utilities, trait definitions, and prelude exports.

/// Common utilities and structures used throughout the Acton framework.
pub(crate) mod common;

pub(crate) mod actor;
pub(crate) mod message;
/// Trait definitions used in the Acton framework.
pub(crate) mod traits;

/// Prelude module for convenient imports.
///
/// This module re-exports commonly used items from the `common` and `traits` modules,
/// as well as the `async_trait` crate.
pub mod prelude {
    pub use acton_ern::*;
    pub use async_trait;

    pub use crate::actor::{ActorConfig, ManagedAgent, Idle, Started};
    pub use crate::common::{ActonApp, AgentHandle, AgentBroker, AgentRuntime, AgentReply};
    pub use crate::message::{BrokerRequest, BrokerRequestEnvelope, OutboundEnvelope, MessageAddress};
    pub use crate::traits::{ActonMessage, Actor, Subscribable, Subscriber, Broker};
}
