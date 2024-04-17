use async_trait::async_trait;
use tokio_util::task::TaskTracker;
use crate::common::{ActorInboxAddress, InternalMessage, LifecycleInboxAddress};
use crate::traits::{ActorContext, LifecycleSupervisor};

pub struct QuasarContext
{
    pub(crate) actor_inbox_address: ActorInboxAddress,
    pub(crate) lifecycle_inbox_address: LifecycleInboxAddress,
    pub(crate) task_tracker: TaskTracker,
}

#[async_trait]
impl ActorContext for QuasarContext {
    fn get_actor_inbox_address(&mut self) -> &mut ActorInboxAddress {
        &mut self.actor_inbox_address
    }

    fn get_task_tracker(&mut self) -> &mut TaskTracker {
        &mut self.task_tracker
    }

    async fn stop(self) -> anyhow::Result<()> {
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
