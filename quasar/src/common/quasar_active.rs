use std::io::Write;
use std::sync::atomic::Ordering;
use std::time::Duration;
use async_trait::async_trait;
use quasar_qrn::Qrn;
use tokio::sync::mpsc::channel;
use tracing::{debug, error, instrument, trace};
use crate::common::{Wormhole, WormholeEntrance, PhotonResponderMap, QuasarHaltSignal, EventHorizonReactor, SingularityWormhole, SingularityWormholeEntrance, SingularitySignalResponderMap, GalacticCoreHaltSignal, QuasarDormant};
use crate::common::*;
use crate::traits::EventHorizon;

pub struct QuasarActive<T: 'static, U: 'static> {
    pub key: Qrn,
    pub state: T,
    pub(crate) singularity_signal_responder_map: Option<SingularitySignalResponderMap<T, U>>,
    singularity_wormhole: SingularityWormhole,
    pub(crate) singularity_wormhole_entrance: SingularityWormholeEntrance,
    galactic_core_halt_signal: GalacticCoreHaltSignal,
    on_start_reactor: EventHorizonReactor<QuasarActive<T, U>>,
    on_stop_reactor: EventHorizonReactor<QuasarActive<T, U>>,
    pub(crate) photon_responder_map: Option<PhotonResponderMap<T, U>>,
    wormhole: Wormhole,
    pub(crate) wormhole_entrance: WormholeEntrance,
    quasar_halt_signal: QuasarHaltSignal,
}

impl<T, U> QuasarActive<T, U> {
    #[instrument(skip(self, photon_responder_map, singularity_signal_response_map), fields(qrn = & self.key.value, self.wormhole.is_closed = & self.wormhole.is_closed()))]
    pub(crate) async fn capture_photons(&mut self, photon_responder_map: PhotonResponderMap<T, U>, singularity_signal_response_map: SingularitySignalResponderMap<T, U>) {
        (self.on_start_reactor)(self);

        loop {
            trace!("photon_responder_map item count: {}", photon_responder_map.len());
            trace!("singularity_signal_response_map item count: {}", singularity_signal_response_map.len());
            // tokio::time::sleep(Duration::from_millis(2)).await;
            // Fetch and process actor messages if available
            while let Ok(photon) = self.wormhole.try_recv() {
                trace!("actor_msg {:?}", photon);
                let type_id = photon.as_any().type_id();
                if let Some(reactor) = photon_responder_map.get(&type_id) {
                    {
                        reactor(self, &*photon);
                    }
                } else {
                    error!("No handler for message type: {:?}", photon);
                }
            }

            // Check lifecycle messages
            if let Ok(singularity_signal) = self.singularity_wormhole.try_recv() {
                let type_id = singularity_signal.as_any().type_id();
                if let Some(reactor) = singularity_signal_response_map.get(&type_id) {
                    reactor(self, &*singularity_signal);
                } else {
                    error!("No handler for message type: {:?}", singularity_signal);
                }
            }
            else {
                //give some time back to the tokio runtime
                tokio::time::sleep(Duration::from_nanos(1)).await;
            }

            // Check the stop condition after processing messages
            if self.quasar_halt_signal.load(Ordering::SeqCst) && self.wormhole.is_empty() {
                trace!("quasar_halt_signal received, exiting capture loop");
                std::io::stdout().flush().expect("Failed to flush stdout");
                break;
            }
        }
        (self.on_stop_reactor)(self);
    }

    pub(crate) fn stop(&self) {
        if !self.quasar_halt_signal.load(Ordering::SeqCst) {
            self.quasar_halt_signal.store(true, Ordering::SeqCst);
        }
    }
}


impl<T: Default + Send + Sync, U: Send + Sync> From<Quasar<QuasarDormant<T, U>>> for Quasar<QuasarActive<T, U>> {
    #[instrument("from dormant to running", skip(value))]
    fn from(value: Quasar<QuasarDormant<T, U>>) -> Quasar<QuasarActive<T, U>> {
        let (actor_inbox_address, actor_inbox) = channel(255);
        let (lifecycle_inbox_address, lifecycle_inbox) = channel(255);
        debug!("{} items in actor_reactor_map", value.entanglement_link.photon_responder_map.len());
        Quasar {
            entanglement_link: QuasarActive {
                singularity_wormhole_entrance: lifecycle_inbox_address,
                singularity_wormhole: lifecycle_inbox,
                galactic_core_halt_signal: GalacticCoreHaltSignal::new(false),
                on_start_reactor: value.entanglement_link.on_start_photon_captures,
                on_stop_reactor: value.entanglement_link.on_stop_photon_captures,
                photon_responder_map: Some(value.entanglement_link.photon_responder_map),
                singularity_signal_responder_map: Some(value.entanglement_link.singularity_signal_responder_map),
                wormhole: actor_inbox,
                wormhole_entrance: actor_inbox_address,
                quasar_halt_signal: QuasarHaltSignal::new(false),
                key: value.entanglement_link.key,
                state: value.entanglement_link.state,
            },
        }
    }
}
#[async_trait]
impl<T: Unpin, U> EventHorizon for QuasarActive<T, U> {
    type Context = EntanglementLink;

    fn get_lifecycle_inbox(&mut self) -> &mut SingularityWormhole {
        &mut self.singularity_wormhole
    }

    fn get_lifecycle_stop_flag(&mut self) -> &mut GalacticCoreHaltSignal {
        &mut self.galactic_core_halt_signal
    }
}


unsafe impl<T, U> Send for QuasarActive<T, U> {}
