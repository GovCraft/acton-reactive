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

pub struct Awake<State: Default + Send + Debug + 'static> {
    pub(crate) on_wake: Box<LifecycleReactor<Awake<State>, State>>,
    pub(crate) on_before_stop: Box<LifecycleReactor<Awake<State>, State>>,
    pub(crate) on_before_stop_async: Option<LifecycleReactorAsync<State>>,
    pub(crate) on_stop: Box<LifecycleReactor<Awake<State>, State>>,
}
impl<State: Default + Send + Debug + 'static> Debug for Awake<State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Awake")
            //          .field("on_wake", &self.on_wake)
            //          .field("on_before_stop", &self.on_before_stop)
            //          .field("on_before_stop_async", &self.on_before_stop_async)
            //          .field("on_stop", &self.on_stop)
            .finish()
    }
}

impl<State: Default + Send + Debug + 'static> Awake<State> {}

impl<State: Default + Send + Debug + 'static> From<Actor<Idle<State>, State>>
    for Actor<Awake<State>, State>
{
    #[instrument("from idle to awake", skip(value))]
    fn from(value: Actor<Idle<State>, State>) -> Actor<Awake<State>, State>
    where
        State: Send + 'static,
    {
        let on_wake = value.ctx.on_wake;
        let on_stop = Box::new(value.ctx.on_stop);
        let on_before_stop = value.ctx.on_before_stop;
        let on_before_stop_async = value.ctx.on_before_stop_async;
        let halt_signal = StopSignal::new(false);
        let parent_return_envelope = value.parent_return_envelope;
        let key = value.key.clone();
        let subordinates = value.subordinates;
        let task_tracker = value.task_tracker.clone();
        let outbox = value.ctx.outbox;
        Actor {
            ctx: Awake {
                on_wake,
                on_before_stop,
                on_before_stop_async,
                on_stop,
                key,
            },
            outbox: Some(outbox),
            parent_return_envelope,
            halt_signal,
            key: value.key,
            state: value.state,
            subordinates,
            task_tracker,
        }
    }
}
