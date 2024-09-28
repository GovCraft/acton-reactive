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

use crate::common::{
    ActorRef, AsyncLifecycleHandler, BrokerRef, HaltSignal, IdleLifecycleHandler, LifecycleHandler,
    ParentRef, ReactorMap,
};
use crate::message::Envelope;
use crate::prelude::SystemReady;

use super::Running;

mod idle;
pub mod running;

pub struct ManagedActor<ActorState, ManagedEntity: Default + Send + Debug + 'static> {
    pub actor_ref: ActorRef,

    pub parent: Option<ParentRef>,

    pub broker: BrokerRef,

    pub halt_signal: HaltSignal,

    pub ern: Ern<UnixTime>,
    pub acton: SystemReady,

    pub entity: ManagedEntity,

    pub(crate) tracker: TaskTracker,

    pub inbox: Receiver<Envelope>,
    /// Reactor called before the actor wakes up.
    pub(crate) before_activate: Box<IdleLifecycleHandler<Idle, ManagedEntity>>,
    /// Reactor called when the actor wakes up.
    pub(crate) on_activate: Box<LifecycleHandler<Running, ManagedEntity>>,
    /// Reactor called just before the actor stops.
    pub(crate) before_stop: Box<LifecycleHandler<Running, ManagedEntity>>,
    /// Reactor called when the actor stops.
    pub(crate) on_stop: Box<LifecycleHandler<Running, ManagedEntity>>,
    /// Asynchronous reactor called just before the actor stops.
    pub(crate) before_stop_async: Option<AsyncLifecycleHandler<ManagedEntity>>,
    /// Map of reactors for handling different message types.
    pub(crate) reactors: ReactorMap<ManagedEntity>,
    _actor_state: std::marker::PhantomData<ActorState>,
}

impl<ActorState, ManagedEntity: Default + Send + Debug + 'static> Debug
for ManagedActor<ActorState, ManagedEntity>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedActor")
            .field("key", &self.ern)
            .finish()
    }
}
