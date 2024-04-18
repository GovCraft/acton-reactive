use std::any::{Any, TypeId};
use std::fmt::Debug;
use async_trait::async_trait;
use quasar_qrn::prelude::*;
use tokio_util::task::TaskTracker;
use crate::common::{WormholeEntrance, SingularityWormhole, SingularityWormholeEntrance, GalacticCoreHaltSignal, Quasar, QuasarDormant, QuasarCore};

//region Traits
pub trait PhotonPacket: Any + Sync + Send + Debug {
    fn as_any(&self) -> &dyn Any;
    fn type_id(&self) -> TypeId { TypeId::of::<Self>() }
}

pub trait SingularitySignal: Any + Sync + Send + Debug {
    fn as_any(&self) -> &dyn Any;
}

//endregion
#[async_trait]
pub trait EventHorizon: Sized + Unpin + 'static {
    /// Actor execution context type
    type Context: Entanglement;
    // fn new() -> Self;

    fn get_lifecycle_inbox(&mut self) -> &mut SingularityWormhole;
    fn get_lifecycle_stop_flag(&mut self) -> &mut GalacticCoreHaltSignal;
    // async fn lifecycle_listen(&mut self, lifecycle_message_reactor_map: SystemMessageReactorMap);
}

#[async_trait]
pub(crate) trait SpookyDistanceTarget {
    fn get_singularity_wormhole_entrance(&mut self) -> &mut SingularityWormholeEntrance;
    async fn send_lifecycle(&mut self, message: impl SingularitySignal) -> anyhow::Result<()> {
        self.get_singularity_wormhole_entrance().send(Box::new(message)).await?;
        Ok(())
    }
}

pub trait ActorFactory {
    fn new_quasar<T: Default + Send + Sync, U: Send + Sync>(&self, actor: T, id: &str) -> Quasar<QuasarDormant<T, QuasarCore, >>;
}

#[async_trait]
pub trait Entanglement: Sized {


    fn get_wormhole_entrance(&mut self) -> &mut WormholeEntrance;
    fn get_task_tracker(&mut self) -> &mut TaskTracker;

    fn key(&self) -> &Qrn;

    async fn emit(&mut self, message: impl PhotonPacket) -> anyhow::Result<()> {
        self.get_wormhole_entrance().send(Box::new(message)).await?;
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

}

