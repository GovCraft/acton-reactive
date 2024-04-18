use std::any::TypeId;
use std::time::SystemTime;
use dashmap::DashMap;
use quasar_qrn::Qrn;
use tracing::{debug, error, instrument};
use crate::common::{PhotonResponder, PhotonResponderMap, EventHorizonReactor, SingularitySignalResponder, SingularitySignalResponderMap};
use crate::common::*;
use crate::traits::{PhotonPacket, SingularitySignal};

pub struct QuasarDormant<T: 'static + Send + Sync, U: 'static + Send + Sync> {
    pub key: Qrn,
    pub state: T,
    pub parent: Option<&'static U>,
    pub(crate) begin_idle_time: SystemTime,
    pub(crate) on_before_start_photon_captures: EventHorizonReactor<Self>,
    pub(crate) on_start_photon_captures: EventHorizonReactor<QuasarActive<T, U>>,
    pub(crate) on_stop_photon_captures: EventHorizonReactor<QuasarActive<T, U>>,
    pub(crate) photon_responder_map: PhotonResponderMap<T, U>,
    pub(crate) singularity_signal_responder_map: SingularitySignalResponderMap<T, U>,
}

impl<T: std::default::Default + Send + Sync, U: Send + Sync> QuasarDormant<T, U> {
    //region elapsed time
    pub fn get_elapsed_idle_time_ms(&self) -> Result<u128, String> {
        match SystemTime::now().duration_since(self.begin_idle_time) {
            Ok(duration) => Ok(duration.as_millis()), // Convert the duration to milliseconds
            Err(_) => Err("System time seems to have gone backwards".to_string()),
        }
    }
    //endregion
    #[instrument(skip(self, actor_message_reactor))]
    pub fn observe<M: PhotonPacket + 'static>(&mut self, actor_message_reactor: impl Fn(&mut QuasarActive<T, U>, &M) + Send + Sync + 'static) -> &mut Self
        where T: Default + Send,
              U: Send {
        // Extract the necessary data from self before moving it into the closure
        let qrn_value = self.key.value.clone(); // Assuming `qrn.value` is cloneable
        debug!("{}", qrn_value);
        // Create a boxed reactor that can be stored in the HashMap.
        let actor_message_reactor_box: PhotonResponder<T, U> = Box::new(move |actor: &mut QuasarActive<T, U>, actor_message: &dyn PhotonPacket| {
            // Attempt to downcast the message to its concrete type.
            if let Some(concrete_msg) = actor_message.as_any().downcast_ref::<M>() {
                actor_message_reactor(actor, concrete_msg);
            } else {
                // If downcasting fails, log a warning.
                error!("Warning: Message type mismatch: {:?}", std::any::type_name::<M>());
            }
        });

        // Use the type ID of the concrete message type M as the key in the handlers map.
        let type_id = TypeId::of::<M>();
        match self.photon_responder_map.insert(type_id, actor_message_reactor_box){
            None => {
                debug!("Inserted with no existing return value")
            }
            Some(_) => {
                debug!("Added to the map")
            }
        };
        debug!("{} reactor in actor_reactor_map", self.photon_responder_map.len());

        // Return self to allow chaining.
        self
    }

    pub fn observe_singularity_signal<M: SingularitySignal + 'static>(&mut self, lifecycle_message_reactor: impl Fn(&mut QuasarActive<T, U>, &M) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        let lifecycle_message_reactor_box: SingularitySignalResponder<T, U> = Box::new(move |actor: &mut QuasarActive<T, U>, lifecycle_message: &dyn SingularitySignal| {
            // Attempt to downcast the message to its concrete type.
            if let Some(concrete_msg) = lifecycle_message.as_any().downcast_ref::<M>() {
                lifecycle_message_reactor(actor, concrete_msg);
            } else {
                // If downcasting fails, log a warning.
                error!("Warning: SystemMessage type mismatch: {:?}", std::any::type_name::<M>());
            }
        });

        // Use the type ID of the concrete message type M as the key in the handlers map.
        let type_id = TypeId::of::<M>();
        self.singularity_signal_responder_map.insert(type_id, lifecycle_message_reactor_box);

        // Return self to allow chaining.
        self
    }

    pub fn on_before_start(&mut self, life_cycle_event_reactor: impl Fn(&QuasarDormant<T, U>) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_before_start_photon_captures = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn on_start(&mut self, life_cycle_event_reactor: impl Fn(&QuasarActive<T, U>) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_start_photon_captures = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn on_stop(&mut self, life_cycle_event_reactor: impl Fn(&QuasarActive<T, U>) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_stop_photon_captures = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn new(qrn: Qrn, state: T) -> QuasarDormant<T, U> {
        QuasarDormant {
            key: qrn,
            state,
            parent: None,
            begin_idle_time: SystemTime::now(),
            on_before_start_photon_captures: Box::new(|_| {}),
            on_start_photon_captures: Box::new(|_| {}),
            on_stop_photon_captures: Box::new(|_| {}),
            photon_responder_map: DashMap::new(),
            singularity_signal_responder_map: DashMap::new(),
        }
    }
}
//endregion

