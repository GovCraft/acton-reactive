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
use crate::traits::{SystemMessage};
use crate::common::{Awake, Envelope};

//region Common Types
pub type SignalReactor<T, U> = dyn for<'a, 'b> Fn(Arc<Mutex<Awake<T, U>>>, &dyn SystemMessage) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> + Send + Sync + 'static;
pub type SignalReactorMap<T, U> = DashMap<TypeId, Box<SignalReactor<T, U>>>;
pub type InboundSignalChannel = Receiver<Box<dyn SystemMessage>>;
pub type OutboundSignalChannel = Sender<Box<dyn SystemMessage>>;

pub type MessageReactorMap<T, U> = DashMap<TypeId, Box<MessageReactor<T, U>>>;
// type MessageReactor<T,U> = dyn Fn(&mut Awake<T, U>, &Envelope) -> Pin<Box<dyn Future<Output=()> + Send>> + Send + 'static;
// pub type MessageReactor<T, U> = dyn for<'a, 'b> Fn(&'a mut Awake<T, U>, &'b Envelope) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> + Send + 'static;
// pub type MessageReactor<T, U> = dyn for<'a, 'b> Fn(Arc<Mutex<Awake<T, U>>>, &'b Envelope) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> + Send + 'static;
pub type MessageReactor<T, U> = dyn for<'a, 'b> Fn(Arc<Mutex<Awake<T, U>>>, &'b Envelope) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> + Send + Sync + 'static;

pub type OutboundChannel = Sender<Envelope>;
pub type InboundChannel = Receiver<Envelope>;
pub type StopSignal = AtomicBool;

pub type LifecycleReactor<T> = dyn Fn(Arc<Mutex<T>>) + Send + Sync;
pub type IdleLifecycleReactor<T> = dyn Fn(&T) + Send + Sync;
// type ActorReactor = Box<dyn Fn(&mut MyActorRunning, &dyn ActorMessage) + Send + Sync>;
// pub type SignalReactor<T, U> = Box<dyn Fn(Arc<Mutex<Awake<T, U>>>, &dyn SystemMessage) + Send + Sync>;
// pub type AsyncResult<'a> = Pin<Box<dyn Future<Output=()> + Send + 'a>>;
//pub type MessageReactor<T, U> = Box<dyn Fn(&mut Awake<T, U>, Envelope<&dyn QuasarMessage>) + Send + Sync>;

//endregion
