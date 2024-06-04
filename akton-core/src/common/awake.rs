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

use crate::common::*;
use crate::common::{Idle, LifecycleReactor};
use std::fmt;
use std::fmt::Formatter;
use tracing::{instrument, warn};

/// Represents the lifecycle state of an actor when it is awake.
///
/// # Type Parameters
/// - `State`: The type representing the state of the actor.
pub struct Awake<State: Default + Sync + Send + Debug + 'static> {
    /// Reactor called when the actor wakes up.
    pub(crate) on_wake: Box<LifecycleReactor<Awake<State>, State>>,
    /// Reactor called just before the actor stops.
    pub(crate) on_before_stop: Box<LifecycleReactor<Awake<State>, State>>,
    /// Asynchronous reactor called just before the actor stops.
    pub(crate) on_before_stop_async: Option<LifecycleReactorAsync<State>>,
    /// Reactor called when the actor stops.
    pub(crate) on_stop: Box<LifecycleReactor<Awake<State>, State>>,
}

/// Custom implementation of the `Debug` trait for the `Awake` struct.
///
/// This implementation provides a formatted output for the `Awake` struct.
impl<State: Default + Sync + Send + Debug + 'static> Debug for Awake<State> {
    /// Formats the `Awake` struct using the given formatter.
    ///
    /// # Parameters
    /// - `f`: The formatter used for writing formatted output.
    ///
    /// # Returns
    /// A result indicating whether the formatting was successful.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Awake")
            .finish()
    }
}

/// Conversion from `Actor<Idle<State>, State>` to `Actor<Awake<State>, State>`.
///
/// This implementation provides a way to transition an actor from the idle state to the awake state.
///
/// # Type Parameters
/// - `State`: The type representing the state of the actor.
impl<State: Default + Send + Sync + Debug + 'static> From<Actor<Idle<State>, State>>
for Actor<Awake<State>, State>
{
    /// Converts an `Actor` from the idle state to the awake state.
    ///
    /// # Parameters
    /// - `value`: The `Actor` instance in the idle state.
    ///
    /// # Returns
    /// A new `Actor` instance in the awake state.
    #[instrument("from idle to awake", skip(value), fields(key=value.key.value,children_in=value.context.children.len()))]
    fn from(value: Actor<Idle<State>, State>) -> Actor<Awake<State>, State>
        where
            State: Send + Sync + 'static,
    {
        tracing::trace!("*");
        // Extract lifecycle reactors and other properties from the idle actor
        let on_wake = value.setup.on_wake;
        let on_stop = value.setup.on_stop;
        let on_before_stop = value.setup.on_before_stop;
        let on_before_stop_async = value.setup.on_before_stop_async;
        let halt_signal = value.halt_signal;
        let parent_return_envelope = value.parent_return_envelope;
        let key = value.key;
        let task_tracker = value.task_tracker;

        // Trace the process and check if the mailbox is closed before conversion
        tracing::trace!("Checking if mailbox is closed before conversion");
        debug_assert!(
            !value.mailbox.is_closed(),
            "Actor mailbox is closed before conversion in From<Actor<Idle<State>, State>>"
        );

        let mailbox = value.mailbox;
        let context = value.context;
        let state = value.state;

        // Trace the conversion process
        // tracing::trace!(
        //     "Converting Actor from Idle to Awake with key: {}",
        //     key.value
        // );
        // tracing::trace!("Checking if mailbox is closed before conversion");
        debug_assert!(
            !mailbox.is_closed(),
            "Actor mailbox is closed in From<Actor<Idle<State>, State>>"
        );
        debug_assert!(
            context
                .supervisor_outbox
                .as_ref()
                .map_or(true, |outbox| !outbox.is_closed()),
            "Supervisor outbox is closed in From<Actor<Idle<State>, State>>"
        );

        // tracing::trace!("Mailbox is not closed, proceeding with conversion");
        if context.children.len() > 0 {
            tracing::trace!("child count before Actor creation {}", context.children.len());
        }
        // Create and return the new actor in the awake state
        Actor {
            setup: Awake {
                on_wake,
                on_before_stop,
                on_before_stop_async,
                on_stop,
            },
            context,
            parent_return_envelope,
            halt_signal,
            key,
            state,
            task_tracker,
            mailbox,
        }
    }
}

