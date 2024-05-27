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

use std::any::TypeId;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;

use crate::common::{Actor, Awake, Context, Envelope};
use crate::traits::AktonMessage;
use dashmap::DashMap;
use tokio::sync::mpsc::{Receiver, Sender};

pub type ReactorMap<T> = DashMap<TypeId, ReactorItem<T>>;

pub enum ReactorItem<T: Clone + Default + Send + Debug + 'static> {
    Signal(Box<SignalReactor<T>>),
    Message(Box<MessageReactor<T>>),
    Future(Box<FutReactor<T>>),
}

pub type MessageReactor<State> =
    dyn for<'a, 'b> Fn(&mut Actor<Awake<State>, State>, &'b Envelope) + Send + Sync + 'static;

pub type SignalReactor<State> = dyn for<'a, 'b> Fn(&mut Actor<Awake<State>, State>, &dyn AktonMessage) -> Fut
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
