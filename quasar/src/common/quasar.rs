use quasar_qrn::Qrn;
use tokio_util::task::TaskTracker;
use crate::common::{DarkSignal, EntanglementLink, QuasarDormant, QuasarActive};
use tracing::{debug, instrument, warn};

pub struct Quasar<S> {
    pub entanglement_link: S,
}


impl<T: Default + Send + Sync, U: Send + Sync> Quasar<QuasarDormant<T, U>> {
    pub(crate) fn new(qrn: Qrn, state:T) -> Self {
        Quasar {
            entanglement_link: QuasarDormant::new(qrn, state)
        }
    }
    #[instrument(skip(dormant_quasar))]
    // Modified Rust function to avoid the E0499 error by preventing simultaneous mutable borrows of actor.ctx
    pub async fn spawn(dormant_quasar: Quasar<QuasarDormant<T, U>>) -> EntanglementLink {

        // Convert the actor from MyActorIdle to MyActorRunning
        let mut actor = dormant_quasar;

        // Handle any pre_start activities
        (actor.entanglement_link.on_before_start_photon_captures)(&actor.entanglement_link);

        // Ensure reactors are correctly assigned
        Self::assign_lifecycle_reactors(&mut actor);

        // Convert to QuasarRunning state
        let mut active_quasar: Quasar<QuasarActive<T, U>> = actor.into();

        // Take reactor maps and inbox addresses before entering async context
        let singularity_signal_responder_map = active_quasar.entanglement_link.singularity_signal_responder_map.take().expect("No lifecycle reactors provided. This should never happen");
        let photon_responder_map = active_quasar.entanglement_link.photon_responder_map.take().expect("No actor message reactors provided. This should never happen");
        debug!("{} items in actor_reactor_map", photon_responder_map.len());

        let actor_inbox_address = active_quasar.entanglement_link.wormhole_entrance.clone();
        assert!(!actor_inbox_address.is_closed(), "Actor inbox address must be valid");

        let lifecycle_inbox_address = active_quasar.entanglement_link.singularity_wormhole_entrance.clone();
        assert!(!lifecycle_inbox_address.is_closed(), "Lifecycle inbox address must be valid");

        let qrn = active_quasar.entanglement_link.key.clone();

        let task_tracker = TaskTracker::new();

        // Spawn task to listen to messages
        task_tracker.spawn(async move {
            active_quasar.entanglement_link.capture_photons(photon_responder_map, singularity_signal_responder_map).await
        });
        task_tracker.close();
        assert!(task_tracker.is_closed(), "Task tracker must be closed after operations");

        // Create a new QuasarContext with pre-extracted data
        EntanglementLink {
            wormhole_entrance: actor_inbox_address,
            singularity_wormhole_entrance: lifecycle_inbox_address,
            task_tracker,
            key: qrn,
        }
    }

    #[instrument(skip(dormant_quasar), fields(qrn=dormant_quasar.entanglement_link.key.value))]
    fn assign_lifecycle_reactors(dormant_quasar: &mut Quasar<QuasarDormant<T, U>>) {
        debug!("assigning_lifeccycle reactors");

        dormant_quasar.entanglement_link.observe_singularity_signal::<DarkSignal>(|active_quasar, lifecycle_message| {
            match lifecycle_message {
                DarkSignal::Stop => {
                    warn!("Received stop message");
                    active_quasar.stop();
                }
            }
        });
    }
}
