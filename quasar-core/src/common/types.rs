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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use dashmap::DashMap;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use crate::traits::{QuasarMessage, SystemMessage};
use crate::common::{Actor, Awake, Context, Envelope};

pub enum ReactorItem<T: Send + Sync + 'static> {
    Signal(Box<SignalReactor<T>>),
    Message(Box<MessageReactor<T>>),
    Future(Box<FutReactor<T>>),
}

pub type ReactorMap<T> = DashMap<TypeId, ReactorItem<T>>;

//region Common Types
pub type SignalReactor<T> = dyn for<'a, 'b> Fn(Actor<Awake<T>>, &dyn QuasarMessage) -> Pin<Box<dyn Future<Output=()> + Send + 'static>> + Send + Sync + 'static;
pub type SignalReactorMap<T> = DashMap<TypeId, Box<SignalReactor<T>>>;
pub type InboundSignalChannel = Receiver<Box<dyn SystemMessage>>;
pub type OutboundSignalChannel = Sender<Box<dyn SystemMessage>>;

pub type MessageReactorMap<T> = DashMap<TypeId, Box<MessageReactor<T>>>;
pub type MessageReactor<T> = dyn for<'a, 'b> Fn(&mut Actor<Awake<T>>, &'b Envelope) + Send + Sync + 'static;
pub type Fut = Pin<Box<dyn Future<Output=()> + Send + 'static>>;
pub type FutReactor<T> = dyn for<'a, 'b> Fn(&mut Actor<Awake<T>>, &'b Envelope) -> Fut + Send + Sync + 'static;
pub type BoxFutReactor<T> = Box<FutReactor<T>>;
pub type PinBoxFutReactor<T> = Pin<BoxFutReactor<T>>;
pub type FutReactorMap<T> = DashMap<TypeId, Box<dyn Fn(&mut Actor<Awake<T>>, &Envelope) -> Pin<Box<dyn Future<Output=()> + Send + Sync>> + Send + Sync>>;
pub type OutboundChannel = Sender<Envelope>;
pub type InboundChannel = Receiver<Envelope>;
pub type StopSignal = AtomicBool;

pub type ContextPool = Vec<Context>;
pub type ActorPool = DashMap<String, ContextPool>;
pub type LifecycleReactor<T> = dyn Fn(&Actor<T>) + Send + Sync;
pub type IdleLifecycleReactor<T> = dyn Fn(&Actor<T>) + Send + Sync;
// type ActorReactor = Box<dyn Fn(&mut MyActorRunning, &dyn ActorMessage) + Send + Sync>;
// pub type SignalReactor<T, U> = Box<dyn Fn(Arc<Mutex<Awake<T, U>>>, &dyn SystemMessage) + Send + Sync>;
// pub type AsyncResult<'a> = Pin<Box<dyn Future<Output=()> + Send + 'a>>;
//pub type MessageReactor<T, U> = Box<dyn Fn(&mut Awake<T, U>, Envelope<&dyn QuasarMessage>) + Send + Sync>;

//endregion
