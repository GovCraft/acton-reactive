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
use std::sync::atomic::AtomicBool;
use dashmap::DashMap;
use tokio::sync::mpsc::{Receiver, Sender};
use crate::traits::{PhotonPacket, SingularitySignal};
use crate::common::QuasarActive;

//region Common Types
pub type SingularitySignalResponderMap<T, U> = DashMap<TypeId, SingularitySignalResponder<T, U>>;
pub type SingularityWormhole = Receiver<Box<dyn SingularitySignal>>;
pub type SingularityWormholeEntrance = Sender<Box<dyn SingularitySignal>>;
// pub type LifecycleTaskHandle = JoinHandle<()>;

// pub type SupervisorInbox = Option<BroadcastReceiver<Box<dyn ActorMessage>>>;
// pub type SupervisorInboxAddress = Option<BroadcastSender<Box<dyn ActorMessage>>>;

pub type PhotonResponderMap<T, U> = DashMap<TypeId, PhotonResponder<T, U>>;
pub type WormholeEntrance = Sender<Box<dyn PhotonPacket>>;
pub type Wormhole = Receiver<Box<dyn PhotonPacket>>;
pub type QuasarHaltSignal = AtomicBool;
pub type GalacticCoreHaltSignal = AtomicBool;
// pub type ActorTaskHandle = JoinHandle<()>;
//endregion

// pub type ActorChildMap<T, U> = DashMap<TypeId, ActorReactor<T, U>>;

// pub type LifecycleEventReactorMut<T, U> = Box<dyn Fn(&QuasarRunning<T, U>, &dyn ActorMessage) + Send + Sync>;
pub type EventHorizonReactor<T> = Box<dyn Fn(&T) + Send + Sync>;
// type ActorReactor = Box<dyn Fn(&mut MyActorRunning, &dyn ActorMessage) + Send + Sync>;
pub type SingularitySignalResponder<T, U> = Box<dyn Fn(&mut QuasarActive<T, U>, &dyn SingularitySignal) + Send + Sync>;
// pub type AsyncResult<'a> = Pin<Box<dyn Future<Output=()> + Send + 'a>>;
pub type PhotonResponder<T, U> = Box<dyn Fn(&mut QuasarActive<T, U>, &dyn PhotonPacket) + Send + Sync>;
//endregion
