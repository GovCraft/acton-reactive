use quasar_qrn::Qrn;
use tokio_util::task::TaskTracker;
use crate::common::{InternalMessage, QuasarContext, QuasarDormant, QuasarRunning};
use Clone;
use tracing::{debug, instrument, warn};

pub struct Quasar<S> {
    pub ctx: S,
}


impl<T: Default + Send + Sync, U: Send + Sync> Quasar<QuasarDormant<T, U>> {
    pub(crate) fn new(qrn: Qrn) -> Self {
        Quasar {
            ctx: QuasarDormant::new(qrn)
        }
    }
    #[instrument(skip(actor))]
    // Modified Rust function to avoid the E0499 error by preventing simultaneous mutable borrows of actor.ctx
    pub async fn spawn(actor: Quasar<QuasarDormant<T, U>>) -> QuasarContext {
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
        debug!("{} items in actor_reactor_map", actor_message_reactor_map.len());

        let actor_inbox_address = actor.ctx.actor_inbox_address.clone();
        assert!(!actor_inbox_address.is_closed(), "Actor inbox address must be valid");

        let lifecycle_inbox_address = actor.ctx.lifecycle_inbox_address.clone();
        assert!(!lifecycle_inbox_address.is_closed(), "Lifecycle inbox address must be valid");

        let qrn = actor.ctx.qrn.clone();

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
            qrn,
        }
    }

    #[instrument(skip(actor), fields(qrn=actor.ctx.qrn.value))]
    fn assign_lifecycle_reactors(actor: &mut Quasar<QuasarDormant<T, U>>) {
        debug!("assigning_lifeccycle reactors");

        actor.ctx.act_on_lifecycle::<InternalMessage>(|actor, lifecycle_message| {
            match lifecycle_message {
                InternalMessage::Stop => {
                    warn!("Received stop message");
                    actor.stop();
                }
            }
        });
    }
}
