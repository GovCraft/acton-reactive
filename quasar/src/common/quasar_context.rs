use std::fmt::Debug;
use async_trait::async_trait;
use tokio_util::task::TaskTracker;
use crate::common::{ActorInboxAddress, InternalMessage, LifecycleInboxAddress, Quasar, QuasarDormant, QuasarRunning, QuasarSystem};
use crate::traits::{ActorContext, ActorFactory, LifecycleSupervisor};
use quasar_qrn::{prelude, Qrn};
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct QuasarContext
{
    pub(crate) actor_inbox_address: ActorInboxAddress,
    pub(crate) lifecycle_inbox_address: LifecycleInboxAddress,
    pub(crate) task_tracker: TaskTracker,
    pub(crate) qrn: Qrn,
}

// impl ActorFactory for QuasarContext {
//     fn new_quasar<T: Default, U>(&self, actor: T, id: &str) -> Quasar<QuasarDormant<T, QuasarSystem, >> {
//         //get the parent if it exists
//         let mut qrn =self.singularity.qrn.clone();
//         qrn.append_part(id);
//
//         let quasar = Quasar::new(qrn);
//         quasar
//     }
//
// }
impl QuasarContext {
    pub fn new_quasar<T: Default + Send + Sync + Debug>(&self, actor: T, id: &str) -> Quasar<QuasarDormant<T, Self>> {
        //get the parent if it exists
        let mut qrn = self.qrn().clone();
        qrn.append_part(id);

        let quasar = Quasar::new(qrn, actor);
        quasar
    }
}

#[async_trait]
impl ActorContext for QuasarContext {
    fn get_actor_inbox_address(&mut self) -> &mut ActorInboxAddress {
        &mut self.actor_inbox_address
    }


    fn get_task_tracker(&mut self) -> &mut TaskTracker {
        &mut self.task_tracker
    }

    fn qrn(&self) -> &Qrn {
        &self.qrn
    }

    #[instrument(skip(self), fields(qrn = self.qrn.value))]
    async fn stop(self) -> anyhow::Result<()> {
        debug!("Sending stop message to lifecycle address");
        self.lifecycle_inbox_address.send(Box::new(InternalMessage::Stop)).await?;
        self.task_tracker.wait().await;
        Ok(())
    }

    fn terminate(&mut self) {
        todo!()
    }

    fn start(&mut self) {
        todo!()
    }
}

#[async_trait]
impl LifecycleSupervisor for QuasarContext {
    fn get_lifecycle_inbox_address(&mut self) -> &mut LifecycleInboxAddress {
        &mut self.lifecycle_inbox_address
    }
}
