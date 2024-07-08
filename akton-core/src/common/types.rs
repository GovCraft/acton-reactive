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

use dashmap::DashMap;
use tokio::sync::mpsc::Sender;

use crate::actors::{ManagedActor, Awake};
use crate::common::ActorRef;
use crate::message::Envelope;
use crate::traits::AktonMessage;

/// A type alias for a map of reactors, indexed by `TypeId`.
pub(crate) type ReactorMap<T> = DashMap<TypeId, ReactorItem<T>>;

/// An enum representing different types of reactors for handling signals, messages, and futures.
/// An enum representing different types of reactors for handling signals, messages, and futures.
pub enum ReactorItem<T: Default + Send + Debug + 'static> {
    /// A signal reactor, which reacts to signals.
    Signal(Box<SignalHandler<T>>),
    /// A message reactor, which reacts to messages.
    Message(Box<MessageReactor<T>>),
    /// A future reactor, which reacts to futures.
    Future(Box<FutureHandler<T>>),
}

/// A type alias for a message reactor function.
pub(crate) type MessageReactor<State> =
    dyn for<'a, 'b> Fn(&mut ManagedActor<Awake<State>, State>, &'b mut Envelope) + Send + Sync + 'static;
/// A type alias for a signal reactor function.
pub type SignalHandler<State> = dyn for<'a, 'b> Fn(&mut ManagedActor<Awake<State>, State>, &dyn AktonMessage) -> FutureBox
    + Send
    + Sync
    + 'static;
/// A type alias for a future reactor function.
pub(crate) type FutureHandler<State> = dyn for<'a, 'b> Fn(&mut ManagedActor<Awake<State>, State>, &'b mut Envelope) -> FutureBox
    + Send
    + Sync
    + 'static;

/// A type alias for a boxed future.
pub(crate) type FutureBox = Pin<Box<dyn Future<Output = ()> + Sync + Send + 'static>>;

/// A type alias for an outbound channel, which sends envelopes.
pub(crate) type Outbox = Sender<Envelope>;

/// A type alias for a stop signal, represented by an atomic boolean.
pub(crate) type HaltSignal = AtomicBool;

/// A type alias for a lifecycle reactor function.
pub(crate) type LifecycleHandler<T, State> = dyn Fn(&ManagedActor<T, State>) + Send;

/// A type alias for an asynchronous lifecycle reactor function.
pub(crate) type AsyncLifecycleHandler<State> =
    Box<dyn Fn(&ManagedActor<Awake<State>, State>) -> FutureBox + Send + Sync + 'static>;

/// A type alias for an idle lifecycle reactor function.
pub(crate) type IdleLifecycleHandler<T, State> = dyn Fn(&ManagedActor<T, State>) + Send;
pub type BrokerRef = ActorRef;
pub type ParentRef = ActorRef;
