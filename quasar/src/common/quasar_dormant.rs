use std::any::TypeId;
use std::time::SystemTime;
use dashmap::DashMap;
use quasar_qrn::Qrn;
use tokio_util::task::TaskTracker;
use crate::common::{ActorReactor, ActorReactorMap, LifecycleEventReactor, LifecycleEventReactorMut, LifecycleReactor, LifecycleReactorMap};
use crate::common::*;
use crate::traits::{ActorMessage, LifecycleMessage};

pub struct QuasarDormant<T: 'static + Send + Sync, U: 'static + Send + Sync> {
    pub qrn: Qrn,
    pub state: T,
    pub parent: Option<&'static U>,
    pub(crate) begin_idle_time: SystemTime,
    pub(crate) on_before_start_reactor: LifecycleEventReactor<Self>,
    pub(crate) on_start_reactor: LifecycleEventReactor<QuasarRunning<T, U>>,
    pub(crate) on_stop_reactor: LifecycleEventReactor<QuasarRunning<T, U>>,
    pub(crate) on_before_message_receive_reactor: LifecycleEventReactorMut<T, U>,
    pub(crate) on_after_message_receive_reactor: LifecycleEventReactor<QuasarRunning<T, U>>,
    pub(crate) actor_reactor_map: ActorReactorMap<T, U>,
    pub(crate) lifecycle_reactor_map: LifecycleReactorMap<T, U>,
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
    pub fn act_on<M: ActorMessage + 'static>(&mut self, actor_message_reactor: impl Fn(&mut QuasarRunning<T, U>, &M) + Sync + 'static + Send) -> &mut Self
        where T: Default + Send,
              U: Send {
        // Extract the necessary data from self before moving it into the closure
        let qrn_value = self.qrn.value.clone(); // Assuming `qrn.value` is cloneable
        assert!(false, "{}", qrn_value);
        // Create a boxed reactor that can be stored in the HashMap.
        let actor_message_reactor_box: ActorReactor<T, U> = Box::new(move |actor: &mut QuasarRunning<T, U>, actor_message: &dyn ActorMessage| {
            // Attempt to downcast the message to its concrete type.
            assert!(false, "{}", qrn_value);
            if let Some(concrete_msg) = actor_message.as_any().downcast_ref::<M>() {
                actor_message_reactor(actor, concrete_msg);
            } else {
                // If downcasting fails, log a warning.
                eprintln!("Warning: Message type mismatch: {:?}", std::any::type_name::<M>());
            }
        });

        // Use the type ID of the concrete message type M as the key in the handlers map.
        let type_id = TypeId::of::<M>();
        self.actor_reactor_map.insert(type_id, actor_message_reactor_box);

        // Return self to allow chaining.
        self
    }



    pub fn act_on_lifecycle<M: LifecycleMessage + 'static>(&mut self, lifecycle_message_reactor: impl Fn(&mut QuasarRunning<T, U>, &M) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        let lifecycle_message_reactor_box: LifecycleReactor<T, U> = Box::new(move |actor: &mut QuasarRunning<T, U>, lifecycle_message: &dyn LifecycleMessage| {
            // Attempt to downcast the message to its concrete type.
            if let Some(concrete_msg) = lifecycle_message.as_any().downcast_ref::<M>() {
                lifecycle_message_reactor(actor, concrete_msg);
            } else {
                // If downcasting fails, log a warning.
                eprintln!("Warning: SystemMessage type mismatch: {:?}", std::any::type_name::<M>());
            }
        });

        // Use the type ID of the concrete message type M as the key in the handlers map.
        let type_id = TypeId::of::<M>();
        self.lifecycle_reactor_map.insert(type_id, lifecycle_message_reactor_box);

        // Return self to allow chaining.
        self
    }

    pub fn on_before_start(&mut self, life_cycle_event_reactor: impl Fn(&QuasarDormant<T, U>) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_before_start_reactor = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn on_start(&mut self, life_cycle_event_reactor: impl Fn(&QuasarRunning<T, U>) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_start_reactor = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn on_stop(&mut self, life_cycle_event_reactor: impl Fn(&QuasarRunning<T, U>) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_stop_reactor = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn new(qrn: Qrn) -> QuasarDormant<T, U> {
        QuasarDormant {
            qrn,
            state: Default::default(),
            parent: None,
            begin_idle_time: SystemTime::now(),
            on_before_start_reactor: Box::new(|_| {}),
            on_start_reactor: Box::new(|_| {}),
            on_stop_reactor: Box::new(|_| {}),
            on_before_message_receive_reactor: Box::new(|_, _| {}),
            on_after_message_receive_reactor: Box::new(|_| {}),
            actor_reactor_map: DashMap::new(),
            lifecycle_reactor_map: DashMap::new(),
        }
    }
}
//endregion

