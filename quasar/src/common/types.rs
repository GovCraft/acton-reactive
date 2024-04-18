use std::any::TypeId;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use dashmap::DashMap;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use crate::traits::{ActorMessage, LifecycleMessage};
use tokio::sync::broadcast::{channel as BroadcastChannel, Receiver as BroadcastReceiver, Sender as BroadcastSender};
use crate::common::QuasarRunning;

//region Common Types
pub type LifecycleReactorMap<T, U> = DashMap<TypeId, LifecycleReactor<T, U>>;
pub type LifecycleInbox = Receiver<Box<dyn LifecycleMessage>>;
pub type LifecycleInboxAddress = Sender<Box<dyn LifecycleMessage>>;
pub type LifecycleTaskHandle = JoinHandle<()>;

pub type SupervisorInbox = Option<BroadcastReceiver<Box<dyn ActorMessage>>>;
pub type SupervisorInboxAddress = Option<BroadcastSender<Box<dyn ActorMessage>>>;

pub type ActorReactorMap<T, U> = DashMap<TypeId, ActorReactor<T, U>>;
pub type ActorInboxAddress = Sender<Box<dyn ActorMessage>>;
pub type ActorInbox = Receiver<Box<dyn ActorMessage>>;
pub type ActorStopFlag = AtomicBool;
pub type LifecycleStopFlag = AtomicBool;
pub type ActorTaskHandle = JoinHandle<()>;
//endregion

pub type ActorChildMap<T, U> = DashMap<TypeId, ActorReactor<T, U>>;

pub type LifecycleEventReactorMut<T: Debug, U: Debug> = Box<dyn Fn(&QuasarRunning<T, U>, &dyn ActorMessage) + Send + Sync>;
pub type LifecycleEventReactor<T: Debug> = Box<dyn Fn(&T) + Send + Sync>;
// type ActorReactor = Box<dyn Fn(&mut MyActorRunning, &dyn ActorMessage) + Send + Sync>;
pub type LifecycleReactor<T: Debug, U: Debug> = Box<dyn Fn(&mut QuasarRunning<T, U>, &dyn LifecycleMessage) + Send + Sync>;
pub type AsyncResult<'a> = Pin<Box<dyn Future<Output=()> + Send + 'a>>;
pub type ActorReactor<T: Debug, U: Debug> = Box<dyn Fn(&mut QuasarRunning<T, U>, &dyn ActorMessage) + Send + Sync>;
//endregion
