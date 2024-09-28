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

use std::any::TypeId;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;

use dashmap::DashMap;
use tokio::sync::mpsc::Sender;

use crate::actor::{ManagedActor, Running};
use crate::common::ActorRef;
use crate::message::Envelope;
use crate::traits::ActonMessage;

/// A type alias for a map of reactors, indexed by `TypeId`.
pub(crate) type ReactorMap<ActorEntity> = DashMap<TypeId, ReactorItem<ActorEntity>>;

/// An enum representing different types of reactors for handling signals, messages, and futures.
pub enum ReactorItem<ActorEntity: Default + Send + Debug + 'static> {
    /// A signal reactor, which reacts to signals.
    SignalReactor(Box<SignalHandler<ActorEntity>>),
    /// A message reactor, which reacts to messages.
    MessageReactor(Box<MessageHandler<ActorEntity>>),
    /// A future reactor, which reacts to futures.
    FutureReactor(Box<FutureHandler<ActorEntity>>),
}

/// A type alias for a message reactor function.
pub(crate) type MessageHandler<ManagedEntity> = dyn for<'a, 'b> Fn(&mut ManagedActor<Running, ManagedEntity>, &'b mut Envelope)
+ Send
+ Sync
+ 'static;
/// A type alias for a signal reactor function.
pub type SignalHandler<ManagedEntity> = dyn for<'a, 'b> Fn(&mut ManagedActor<Running, ManagedEntity>, &dyn ActonMessage) -> FutureBox
+ Send
+ Sync
+ 'static;
/// A type alias for a future reactor function.
pub(crate) type FutureHandler<ManagedEntity> = dyn for<'a, 'b> Fn(&mut ManagedActor<Running, ManagedEntity>, &'b mut Envelope) -> FutureBox
+ Send
+ Sync
+ 'static;

/// A type alias for a boxed future.
pub(crate) type FutureBox = Pin<Box<dyn Future<Output=()> + Sync + Send + 'static>>;

/// A type alias for an outbound channel, which sends envelopes.
pub(crate) type Outbox = Sender<Envelope>;

/// A type alias for a stop signal, represented by an atomic boolean.
pub(crate) type HaltSignal = AtomicBool;

/// A type alias for a lifecycle reactor function.
pub(crate) type LifecycleHandler<ActorEntity, ManagedEntity> =
dyn Fn(&ManagedActor<ActorEntity, ManagedEntity>) + Send;

/// A type alias for an asynchronous lifecycle reactor function.
pub(crate) type AsyncLifecycleHandler<ManagedEntity> =
Box<dyn Fn(&ManagedActor<Running, ManagedEntity>) -> FutureBox + Send + Sync + 'static>;

/// A type alias for an idle lifecycle reactor function.
pub(crate) type IdleLifecycleHandler<ActorEntity, ManagedEntity> =
dyn Fn(&ManagedActor<ActorEntity, ManagedEntity>) + Send;
pub type BrokerRef = ActorRef;
pub type ParentRef = ActorRef;
