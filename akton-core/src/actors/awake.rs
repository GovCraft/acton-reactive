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

use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;

use tracing::{instrument, warn};

use crate::actors::managed_actor::ManagedActor;
use crate::actors::Idle;
use crate::common::{LifecycleHandler, AsyncLifecycleHandler};
use crate::traits::Actor;

/// Represents the lifecycle state of an actor when it is awake.
///
/// # Type Parameters
/// - `State`: The type representing the state of the actor.
pub struct Awake;


// /// Conversion from `Actor<Idle, State>` to `Actor<Awake<State>, State>`.
// ///
// /// This implementation provides a way to transition an actor from the idle state to the awake state.
// ///
// /// # Type Parameters
// /// - `State`: The type representing the state of the actor.
// impl From<ManagedActor<Idle, State>>
// for ManagedActor<Awake<State>, State>
// {
//     /// Converts an `Actor` from the idle state to the awake state.
//     ///
//     /// # Parameters
//     /// - `value`: The `Actor` instance in the idle state.
//     ///
//     /// # Returns
//     /// A new `Actor` instance in the awake state.
//     #[instrument("from idle to awake", skip(value), fields(
//         key = value.key, children_in = value.actor_ref.children().len()
//     ))]
//     fn from(value: ManagedActor<Idle, State>) -> ManagedActor<Awake<State>, State>
//     where
//         State: Send + 'static,
//     {
//         tracing::trace!("*");
//         // Extract lifecycle reactors and other properties from the idle actor
//         let on_wake = value.on_activate;
//         let on_stop = value.on_stop;
//         let on_before_stop = value.before_stop;
//         let on_before_stop_async = value.before_stop_async;
//         let halt_signal = value.halt_signal;
//         let parent_return_envelope = value.parent;
//         let key = value.key;
//         let task_tracker = value.tracker;
//         let akton = value.akton;
//
//         // Trace the process and check if the mailbox is closed before conversion
//         tracing::trace!("Checking if mailbox is closed before conversion");
//         debug_assert!(
//             !value.inbox.is_closed(),
//             "Actor mailbox is closed before conversion in From<Actor<Idle, State>>"
//         );
//
//         let mailbox = value.inbox;
//         let context = value.actor_ref;
//         let state = value.entity;
//         let broker = value.broker;
//
//         // Trace the conversion process
//         // tracing::trace!(
//         //     "Converting Actor from Idle to Awake with key: {}",
//         //     key.value
//         // );
//         // tracing::trace!("Checking if mailbox is closed before conversion");
//         debug_assert!(
//             !mailbox.is_closed(),
//             "Actor mailbox is closed in From<Actor<Idle, State>>"
//         );
//
//         // tracing::trace!("Mailbox is not closed, proceeding with conversion");
//         if context.children().is_empty() {
//             tracing::trace!(
//                 "child count before Actor creation {}",
//                 context.children().len()
//             );
//         }
//         // Create and return the new actor in the awake state
//         // ManagedActor {
//         //     // setup: Awake {
//         //     //     on_wake,
//         //     //     on_before_stop,
//         //     //     on_before_stop_async,
//         //     //     on_stop,
//         //     // },
//         //     actor_ref: context,
//         //     parent: parent_return_envelope,
//         //     halt_signal,
//         //     key,
//         //     akton,
//         //     entity: state,
//         //     tracker: task_tracker,
//         //     inbox: mailbox,
//         //     before_activate: Box::new(()),
//         //     on_activate: on_wake,
//         //     before_stop: on_before_stop,
//         //     on_stop,
//         //     before_stop_async: on_before_stop_async,
//         //     broker,
//         //     reactors: Default::default(),
//         // }
//     }
// }
