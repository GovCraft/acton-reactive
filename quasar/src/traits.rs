use std::any::{Any, TypeId};
use std::fmt::Debug;
use async_trait::async_trait;
use quasar_qrn::prelude::*;
use tokio_util::task::TaskTracker;
use crate::common::{ActorInboxAddress, LifecycleInbox, LifecycleInboxAddress, LifecycleStopFlag, Quasar, QuasarDormant, QuasarSystem};

//region Traits
pub trait ActorMessage: Any + Sync + Send + Debug {
    fn as_any(&self) -> &dyn Any;
    fn type_id(&self) -> TypeId { TypeId::of::<Self>() }
}

pub trait LifecycleMessage: Any + Sync + Send + Debug {
    fn as_any(&self) -> &dyn Any;
}

//endregion
#[async_trait]
pub trait Actor: Sized + Unpin + 'static {
    /// Actor execution context type
    type Context: ActorContext;
    // fn new() -> Self;

    fn get_lifecycle_inbox(&mut self) -> &mut LifecycleInbox;
    fn get_lifecycle_stop_flag(&mut self) -> &mut LifecycleStopFlag;
    // async fn lifecycle_listen(&mut self, lifecycle_message_reactor_map: SystemMessageReactorMap);
}

pub trait KnownQuasar {
    fn qrn(&self) -> &Qrn;
}

pub trait QuasarFactory {
    fn new_quasar<S>(&self) -> Quasar<S>;
}

pub trait IdleActor {
    // type State: IdleState;
    fn new() -> Self where Self: Sized;
}

pub trait IdleState {}

#[async_trait]
pub(crate) trait LifecycleSupervisor {
    fn get_lifecycle_inbox_address(&mut self) -> &mut LifecycleInboxAddress;
    async fn send_lifecycle(&mut self, message: impl LifecycleMessage) -> anyhow::Result<()> {
        self.get_lifecycle_inbox_address().send(Box::new(message)).await?;
        Ok(())
    }
}

pub trait ActorFactory {
    fn new_quasar<T: Default + Send + Sync, U: Send + Sync>(&self, actor: T, id: &str) -> Quasar<QuasarDormant<T, QuasarSystem, >>;
}

#[async_trait]
pub trait ActorContext: Sized {
    fn get_actor_inbox_address(&mut self) -> &mut ActorInboxAddress;
    fn get_task_tracker(&mut self) -> &mut TaskTracker;

    fn qrn(&self) -> &Qrn;

    async fn send(&mut self, message: impl ActorMessage) -> anyhow::Result<()> {
        self.get_actor_inbox_address().send(Box::new(message)).await?;
        Ok(())
    }

    /// Immediately stop processing incoming messages and switch to a
    /// `stopping` state. This only affects actors that are currently
    /// `running`. Future attempts to queue messages will fail.
    async fn stop(self) -> anyhow::Result<()>;
    /// Terminate actor execution unconditionally. This sets the actor
    /// into the `stopped` state. This causes future attempts to queue
    /// messages to fail.
    fn terminate(&mut self);

    fn start(&mut self);
    // Retrieve the current Actor execution state.
    // fn state(&self) -> ActorState;
}

