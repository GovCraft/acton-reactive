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

use std::any::TypeId;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::common::{Actor, Awake, Context, Envelope};
use crate::traits::{QuasarMessage, SystemMessage};
use dashmap::DashMap;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;

#[derive(Debug)]
pub(crate) enum SupervisorMessage {
    PoolEmit(Box<dyn QuasarMessage + Send + Sync + 'static>),
}
impl QuasarMessage for SupervisorMessage {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
pub type ReactorMap<T> = DashMap<TypeId, ReactorItem<T>>;

pub enum ReactorItem<T: Default + Send + Debug + 'static> {
    Signal(Box<SignalReactor<T>>),
    Message(Box<MessageReactor<T>>),
    Future(Box<FutReactor<T>>),
}

pub type MessageReactor<State> =
    dyn for<'a, 'b> Fn(&mut Actor<Awake<State>, State>, &'b Envelope) + Send + Sync + 'static;

pub type SignalReactor<State> = dyn for<'a, 'b> Fn(&mut Actor<Awake<State>, State>, &dyn QuasarMessage) -> Fut
    + Send
    + Sync
    + 'static;
pub type FutReactor<State> = dyn for<'a, 'b> Fn(&mut Actor<Awake<State>, State>, &'b Envelope) -> Fut
    + Send
    + Sync
    + 'static;

pub type Fut = Pin<Box<dyn Future<Output = ()> + Sync + Send + 'static>>;
// pub type BoxFutReactor<T> = Box<FutReactor<T>>;
// pub type PinBoxFutReactor<T> = Pin<BoxFutReactor<T>>;

pub type OutboundChannel = Sender<Envelope>;
pub type InboundChannel = Receiver<Envelope>;
pub type StopSignal = AtomicBool;

pub type ContextPool = DashMap<String, Context>;
pub type ActorPool = DashMap<String, ContextPool>;
pub type LifecycleReactor<T, State> = dyn Fn(&Actor<T, State>) + Send;
pub type LifecycleReactorAsync<State> =
    Box<dyn for<'a, 'b> Fn(&Actor<Awake<State>, State>) -> Fut + Send + Sync + 'static>;
pub type IdleLifecycleReactor<T, State> = dyn Fn(&Actor<T, State>) + Send;
