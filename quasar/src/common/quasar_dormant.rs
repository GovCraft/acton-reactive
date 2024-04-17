use std::any::TypeId;
use std::time::SystemTime;
use dashmap::DashMap;
use quasar_qrn::Qrn;
use tokio_util::task::TaskTracker;
use crate::common::{ActorReactor, ActorReactorMap, LifecycleEventReactor, LifecycleEventReactorMut, LifecycleReactor, LifecycleReactorMap};
use crate::common::*;
use crate::traits::{ActorMessage, LifecycleMessage};

pub struct QuasarDormant<T: 'static,U: 'static> {
    pub qrn: Qrn,
    pub state: Option<&'static T>,
    pub parent: Option<&'static U>,
    pub(crate) begin_idle_time: SystemTime,
    pub(crate) on_before_start_reactor: LifecycleEventReactor<Self>,
    pub(crate) on_start_reactor: LifecycleEventReactor<QuasarRunning<T,U>>,
    pub(crate) on_stop_reactor: LifecycleEventReactor<QuasarRunning<T,U>>,
    pub(crate) on_before_message_receive_reactor: LifecycleEventReactorMut<T,U>,
    pub(crate) on_after_message_receive_reactor: LifecycleEventReactor<QuasarRunning<T,U>>,
    pub(crate) actor_reactor_map: ActorReactorMap<T,U>,
    pub(crate) lifecycle_reactor_map: LifecycleReactorMap<T,U>,
}

impl<T,U> Quasar<QuasarDormant<T,U>> {
    pub(crate) fn new(qrn: Qrn) -> Self {
        Quasar {
            ctx: QuasarDormant::new(qrn)
        }
    }

    // Modified Rust function to avoid the E0499 error by preventing simultaneous mutable borrows of actor.ctx
    pub async fn spawn(actor: Quasar<QuasarDormant<T,U>>) -> QuasarContext {
        // Ensure the actor is initially in a dormant state
        assert!(matches!(actor.ctx, ref QuasarDormant), "Actor must be dormant to spawn");

        // Convert the actor from MyActorIdle to MyActorRunning
        let mut actor = actor;

        // Handle any pre_start activities
        let pre_start_result = (actor.ctx.on_before_start_reactor)(&actor.ctx);
        assert_eq!(pre_start_result, (), "Pre-start activities failed");

        // Ensure reactors are correctly assigned
        Self::assign_lifecycle_reactors(&mut actor);

        // Convert to QuasarRunning state
        let mut actor: Quasar<QuasarRunning<T, U>> = actor.into();
        assert!(matches!(actor.ctx, ref QuasarRunning), "Actor must be in running state after conversion");

        // Take reactor maps and inbox addresses before entering async context
        let lifecycle_message_reactor_map = actor.ctx.lifecycle_message_reactor_map.take().expect("No lifecycle reactors provided. This should never happen");
        let actor_message_reactor_map = actor.ctx.actor_message_reactor_map.take().expect("No actor message reactors provided. This should never happen");

        let actor_inbox_address = actor.ctx.actor_inbox_address.clone();
        assert!(!actor_inbox_address.is_closed(), "Actor inbox address must be valid");

        let lifecycle_inbox_address = actor.ctx.lifecycle_inbox_address.clone();
        assert!(!lifecycle_inbox_address.is_closed(), "Lifecycle inbox address must be valid");

        let mut ctx = actor.ctx;
        let task_tracker = TaskTracker::new();

        // Spawn task to listen to actor and lifecycle messages
        task_tracker.spawn(async move {
            ctx.actor_listen(actor_message_reactor_map, lifecycle_message_reactor_map).await
        });
        task_tracker.close();
        assert!(task_tracker.is_closed(), "Task tracker must be closed after operations");

        // Create a new QuasarContext with pre-extracted data
        QuasarContext {
            actor_inbox_address,
            lifecycle_inbox_address,
            task_tracker,
        }
    }


    fn assign_lifecycle_reactors(actor: &mut Quasar<QuasarDormant<T, U>>) {
        actor.ctx.act_on_lifecycle::<InternalMessage>(|actor, lifecycle_message| {
            match lifecycle_message {
                InternalMessage::Stop => {
                    actor.stop();
                }
            }
        });
    }
}

impl<T,U> QuasarDormant<T, U> {
    //region elapsed time
    pub fn get_elapsed_idle_time_ms(&self) -> Result<u128, String> {
        match SystemTime::now().duration_since(self.begin_idle_time) {
            Ok(duration) => Ok(duration.as_millis()), // Convert the duration to milliseconds
            Err(_) => Err("System time seems to have gone backwards".to_string()),
        }
    }
    //endregion
    pub fn act_on<M: ActorMessage + 'static>(&mut self, actor_message_reactor: impl Fn(&mut QuasarRunning<T,U>, &M) + Sync + 'static + Send) -> &mut Self {
        // Create a boxed reactor that can be stored in the HashMap.
        let actor_message_reactor_box: ActorReactor<T,U> = Box::new(move |actor: &mut QuasarRunning<T,U>, actor_message: &dyn ActorMessage| {
            // Attempt to downcast the message to its concrete type.
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

    pub fn act_on_lifecycle<M: LifecycleMessage + 'static>(&mut self, lifecycle_message_reactor: impl Fn(&mut QuasarRunning<T,U>, &M) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        let lifecycle_message_reactor_box: LifecycleReactor<T,U> = Box::new(move |actor: &mut QuasarRunning<T,U>, lifecycle_message: &dyn LifecycleMessage| {
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

    pub fn on_before_start(&mut self, life_cycle_event_reactor: impl Fn(&QuasarDormant<T,U>) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_before_start_reactor = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn on_start(&mut self, life_cycle_event_reactor: impl Fn(&QuasarRunning<T,U>) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_start_reactor = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn on_stop(&mut self, life_cycle_event_reactor: impl Fn(&QuasarRunning<T,U>) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_stop_reactor = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn new(qrn: Qrn) -> QuasarDormant<T, U> {
        QuasarDormant {
            qrn,
            state: None,
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

