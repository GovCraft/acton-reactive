/*
 *
 *  * Copyright (c) 2024 Govcraft.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

use std::fmt::Debug;

use crate::common::*;
use crate::common::{Idle, LifecycleReactor};
use std::fmt;
use std::fmt::Formatter;
use tracing::{instrument, warn};

pub struct Awake<State: Clone + Default + Send + Debug + 'static> {
    pub(crate) on_wake: Box<LifecycleReactor<Awake<State>, State>>,
    pub(crate) on_before_stop: Box<LifecycleReactor<Awake<State>, State>>,
    pub(crate) on_before_stop_async: Option<LifecycleReactorAsync<State>>,
    pub(crate) on_stop: Box<LifecycleReactor<Awake<State>, State>>,
}
impl<State: Clone + Default + Send + Debug + 'static> Debug for Awake<State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Awake")
            //          .field("on_wake", &self.on_wake)
            //          .field("on_before_stop", &self.on_before_stop)
            //          .field("on_before_stop_async", &self.on_before_stop_async)
            //          .field("on_stop", &self.on_stop)
            .finish()
    }
}

impl<State: Clone + Default + Send + Debug + 'static> Awake<State> {}

impl<State: Clone + Default + Send + Debug + 'static> From<Actor<Idle<State>, State>>
    for Actor<Awake<State>, State>
{
    #[instrument("from idle to awake", skip(value))]
    fn from(value: Actor<Idle<State>, State>) -> Actor<Awake<State>, State>
    where
        State: Send + 'static,
    {
        let on_wake = value.ctx.on_wake;
        let on_stop = value.ctx.on_stop;
        let on_before_stop = value.ctx.on_before_stop;
        let on_before_stop_async = value.ctx.on_before_stop_async;
        let halt_signal = value.halt_signal;
        let parent_return_envelope = value.parent_return_envelope;
        let key = value.key;
        let task_tracker = value.task_tracker;
        tracing::trace!("Checking if mailbox is closed before conversion");
        // Add assertions to check if the mailboxes are not closed
        debug_assert!(
            !value.mailbox.is_closed(),
            "Actor mailbox is closed before conversion in From<Actor<Idle<State>, State>>"
        );
        let mailbox = value.mailbox;
        let context = value.context;
        let state = value.state;
        tracing::trace!(
            "Converting Actor from Idle to Awake with key: {}",
            key.value
        );
        tracing::trace!("Checking if mailbox is closed before conversion");
        // Add assertions to check if the mailboxes are not closed
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
        tracing::trace!("Mailbox is not closed, proceeding with conversion");
        Actor {
            ctx: Awake {
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
