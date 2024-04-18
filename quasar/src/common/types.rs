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
