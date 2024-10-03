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

use acton_ern::prelude::*;
use tokio::sync::mpsc::Receiver;
use tokio_util::task::TaskTracker;

pub use idle::Idle;

use crate::common::{AgentHandle, AsyncLifecycleHandler, BrokerRef, HaltSignal, ParentRef, ReactorMap};
use crate::message::Envelope;
use crate::prelude::AgentRuntime;

mod idle;
pub mod started;

/// A managed agent is a wrapper around an actor that provides a set of lifecycle hooks and
///  message handling reactors.
pub struct ManagedAgent<AgentState, ManagedAgent: Default + Send + Debug + 'static> {
    pub(crate) handle: AgentHandle,

    pub(crate) parent: Option<ParentRef>,

    pub(crate) broker: BrokerRef,

    pub(crate) halt_signal: HaltSignal,

    pub(crate) id: Ern,
    pub(crate) runtime: AgentRuntime,
    /// The actor model.
    pub model: ManagedAgent,

    pub(crate) tracker: TaskTracker,

    pub(crate) inbox: Receiver<Envelope>,
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

// implement getter functions for ManagedAgent
impl<ActorState, ManagedEntity: Default + Send + Debug + 'static> ManagedAgent<ActorState, ManagedEntity> {
    /// Returns the unique identifier of the actor.
    pub fn id(&self) -> &Ern {
        &self.id
    }
    /// Returns the name of the actor.
    pub fn name(&self) -> &str {
        self.id.root.as_str()
    }

    /// Returns the handle of the actor.
    pub fn handle(&self) -> &AgentHandle {
        &self.handle
    }

    /// Returns the parent of the actor.
    pub fn parent(&self) -> &Option<ParentRef> {
        &self.parent
    }
    /// Returns the broker of the actor.
    pub fn broker(&self) -> &BrokerRef {
        &self.broker
    }
    /// Returns the app runtime.
    pub fn runtime(&self) -> &AgentRuntime {
        &self.runtime
    }
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
