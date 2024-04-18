use std::fmt::Debug;
use async_trait::async_trait;
use tokio_util::task::TaskTracker;
use crate::common::{WormholeEntrance, DarkSignal, SingularityWormholeEntrance, Quasar, QuasarDormant};
use crate::traits::{Entanglement, SpookyDistanceTarget};
use quasar_qrn::Qrn;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct EntanglementLink
{
    pub(crate) wormhole_entrance: WormholeEntrance,
    pub(crate) singularity_wormhole_entrance: SingularityWormholeEntrance,
    pub(crate) task_tracker: TaskTracker,
    pub(crate) key: Qrn,
}

impl EntanglementLink {
    pub fn new_quasar<T: Default + Send + Sync + Debug>(&self, actor: T, id: &str) -> Quasar<QuasarDormant<T, Self>> {

        //append to the qrn
        let mut qrn = self.key().clone();
        qrn.append_part(id);

        Quasar::new(qrn, actor)
    }
}

#[async_trait]
impl Entanglement for EntanglementLink {
    fn get_wormhole_entrance(&mut self) -> &mut WormholeEntrance {
        &mut self.wormhole_entrance
    }


    fn get_task_tracker(&mut self) -> &mut TaskTracker {
        &mut self.task_tracker
    }

    fn key(&self) -> &Qrn {
        &self.key
    }

    #[instrument(skip(self), fields(qrn = self.key.value))]
    async fn stop(self) -> anyhow::Result<()> {
        debug!("Sending stop message to lifecycle address");
        self.singularity_wormhole_entrance.send(Box::new(DarkSignal::Stop)).await?;
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
impl SpookyDistanceTarget for EntanglementLink {
    fn get_singularity_wormhole_entrance(&mut self) -> &mut SingularityWormholeEntrance {
        &mut self.singularity_wormhole_entrance
    }
}
