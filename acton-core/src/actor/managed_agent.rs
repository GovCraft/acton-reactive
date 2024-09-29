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

use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;

use acton_ern::{Ern, UnixTime};
use tokio::sync::mpsc::Receiver;
use tokio_util::task::TaskTracker;

pub use idle::Idle;

use crate::common::{AgentHandle, AsyncLifecycleHandler, BrokerRef, HaltSignal, ParentRef, ReactorMap};
use crate::message::Envelope;
use crate::prelude::AgentRuntime;

mod idle;
pub mod started;

pub struct ManagedAgent<AgentState, ManagedAgent: Default + Send + Debug + 'static> {
    pub handle: AgentHandle,

    pub parent: Option<ParentRef>,

    pub broker: BrokerRef,

    pub halt_signal: HaltSignal,

    pub id: Ern<UnixTime>,
    pub runtime: AgentRuntime,

    pub model: ManagedAgent,

    pub(crate) tracker: TaskTracker,

    pub inbox: Receiver<Envelope>,
    /// Reactor called when the actor wakes up but before listening begins.
    pub(crate) before_start: AsyncLifecycleHandler<ManagedAgent>,
    /// Reactor called when the actor wakes up but before listening begins.
    pub(crate) after_start: AsyncLifecycleHandler<ManagedAgent>,
    /// Reactor called just before the actor stops listening for messages.
    pub(crate) before_stop: AsyncLifecycleHandler<ManagedAgent>,
    /// Reactor called when the actor stops listening for messages.
    pub(crate) after_stop: AsyncLifecycleHandler<ManagedAgent>,
    /// Map of reactors for handling different message types.
    pub(crate) reactors: ReactorMap<ManagedAgent>,
    _actor_state: std::marker::PhantomData<AgentState>,
}

impl<ActorState, ManagedEntity: Default + Send + Debug + 'static> Debug
for ManagedAgent<ActorState, ManagedEntity>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedActor")
            .field("key", &self.id)
            .finish()
    }
}
