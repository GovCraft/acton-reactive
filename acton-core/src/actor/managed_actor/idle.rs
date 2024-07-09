use std::any::TypeId;
use std::fmt::Debug;
use std::mem;

use dashmap::DashMap;
use tokio::sync::mpsc::channel;
use tracing::{error, event, info, instrument, Level, trace};

use crate::actor::{ActorConfig, ManagedActor, Running};
use crate::common::{ActorRef, ActonInner, SystemReady, Envelope, FutureBox, MessageHandler, OutboundEnvelope, ReactorItem};
use crate::message::EventRecord;
use crate::prelude::{Actor, ActonMessage};

pub struct Idle;



impl<ManagedEntity: Default + Send + Debug + 'static> ManagedActor<Idle, ManagedEntity> {
    #[instrument(skip(self, message_handler))]
    pub fn act_on<M: ActonMessage + Clone + 'static>(
        &mut self,
        message_handler: impl Fn(&mut ManagedActor<Running, ManagedEntity>, &mut EventRecord<M>)
        + Send
        + Sync
        + 'static,
    ) -> &mut Self {
        let type_id = TypeId::of::<M>();
        trace!(type_name = std::any::type_name::<M>(), type_id = ?type_id);
        // Create a boxed handler for the message type.
        let handler: Box<MessageHandler<ManagedEntity>> = Box::new(
            move |actor: &mut ManagedActor<Running, ManagedEntity>, envelope: &mut Envelope| {
                let envelope_type_id = envelope.message.as_any().type_id();
                trace!(
                "Attempting to downcast message: expected_type_id = {:?}, envelope_type_id = {:?}",
                type_id, envelope_type_id
            );
                if let Some(concrete_msg) = downcast_message::<M>(&*envelope.message) {
                    let message = concrete_msg.clone();
                    let sent_time = envelope.sent_time;
                    let return_address = OutboundEnvelope::new(envelope.return_address.clone());
                    let event_record = &mut EventRecord {
                        message,
                        sent_time,
                        return_address,
                    };
                    message_handler(actor, event_record);
                    Box::pin(())
                } else {
                    Box::pin({
                        error!(
                        "Message type mismatch: expected {:?}",
                        std::any::type_name::<M>()
                    );
                    })
                };
            },
        );

        // Insert the handler into the reactors map.
        let _ = self.reactors.insert(type_id, ReactorItem::MessageReactor(handler));

        self
    }

    /// Adds an asynchronous message handler for a specific message type.
    ///
    /// # Parameters
    /// - `message_processor`: The function to handle the message.
    #[instrument(skip(self, message_processor))]
    pub fn act_on_async<M>(
        &mut self,
        message_processor: impl for<'a> Fn(&'a mut ManagedActor<Running, ManagedEntity>, &'a mut EventRecord<M>) -> FutureBox
        + Send
        + Sync
        + 'static,
    ) -> &mut Self
    where
        M: ActonMessage + Clone + Send + Sync + 'static,
    {
        let type_id = TypeId::of::<M>();
        trace!(type_name=std::any::type_name::<M>(),type_id=?type_id);
        // Create a boxed handler for the message type.
        let handler_box = Box::new(
            move |actor: &mut ManagedActor<Running, ManagedEntity>, envelope: &mut Envelope| -> FutureBox {
                let envelope_type_id = envelope.message.as_any().type_id();
                trace!(
                "Attempting to downcast message: expected_type_id = {:?}, envelope_type_id = {:?}",
                type_id, envelope_type_id
            );
                if let Some(concrete_msg) = downcast_message::<M>(&*envelope.message) {
                    trace!("Downcast message to name {} and concrete type: {:?}",std::any::type_name::<M>(), type_id);

                    let message = concrete_msg.clone();
                    let sent_time = envelope.sent_time;
                    let mut event_record = {
                        if let Some(parent) = &actor.parent {
                            let return_address = parent.return_address();
                            EventRecord {
                                message,
                                sent_time,
                                return_address,
                            }
                        } else {
                            let return_address = actor.actor_ref.return_address();
                            EventRecord {
                                message,
                                sent_time,
                                return_address,
                            }
                        }
                    };

                    // Call the user-provided function and get the future.
                    let user_future = message_processor(actor, &mut event_record);

                    // Automatically box and pin the user future.
                    Box::pin(user_future)
                } else {
                    error!(type_name=std::any::type_name::<M>(),"Should never get here, message failed to downcast");
                    // Return an immediately resolving future if downcast fails.
                    Box::pin(async {})
                }
            },
        );

        // Insert the handler into the reactors map.
        let _ = &self
            .reactors
            .insert(type_id, ReactorItem::FutureReactor(handler_box));
        self
    }


    /// Sets the reactor to be called before the actor wakes up.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn before_activate(
        &mut self,
        life_cycle_event_reactor: impl Fn(&ManagedActor<Idle, ManagedEntity>) + Send + Sync + 'static,
    ) -> &mut Self {
        self.before_activate = Box::new(life_cycle_event_reactor);
        self
    }

    /// Sets the reactor to be called when the actor wakes up.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn on_activate(
        &mut self,
        life_cycle_event_reactor: impl Fn(&ManagedActor<Running, ManagedEntity>) + Send + Sync + 'static,
    ) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_activate = Box::new(life_cycle_event_reactor);
        self
    }

    /// Sets the reactor to be called when the actor stops.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn on_stop(
        &mut self,
        life_cycle_event_reactor: impl Fn(&ManagedActor<Running, ManagedEntity>) + Send + Sync + 'static,
    ) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_stop = Box::new(life_cycle_event_reactor);
        self
    }

    /// Sets the reactor to be called just before the actor stops.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn before_stop(
        &mut self,
        life_cycle_event_reactor: impl Fn(&ManagedActor<Running, ManagedEntity>) + Send + Sync + 'static,
    ) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.before_stop = Box::new(life_cycle_event_reactor);
        self
    }

    /// Sets the asynchronous reactor to be called just before the actor stops.
    ///
    /// # Parameters
    /// - `f`: The asynchronous function to be called.
    pub fn before_stop_async<F>(&mut self, f: F) -> &mut Self
    where
        F: for<'b> Fn(&'b ManagedActor<Running, ManagedEntity>) -> FutureBox + Send + Sync + 'static,
    {
        self.before_stop_async = Some(Box::new(f));
        self
    }


    /// Creates and supervises a new actor with the given ID and state.
    ///
    /// # Parameters
    /// - `id`: The identifier for the new actor.
    ///
    /// # Returns
    /// A new `Actor` instance in the idle state.
    #[instrument(skip(self))]
    pub async fn create_child(&self) -> ManagedActor<Idle, ManagedEntity> {
        let actor = ManagedActor::new(&Some(self.acton.clone()), None).await;
        actor
    }

    #[instrument]
    pub(crate) async fn new(acton: &Option<SystemReady>, config: Option<ActorConfig>) -> Self {
        let mut managed_actor: ManagedActor<Idle, ManagedEntity> = ManagedActor::default();

        if let Some(config) = &config {
            managed_actor.actor_ref.set_ern(config.ern());
            managed_actor.parent = config.parent().clone();
            managed_actor.actor_ref.broker = Box::new(config.get_broker().clone());
        }

        debug_assert!(!managed_actor.inbox.is_closed(), "Actor mailbox is closed in new");

        trace!("NEW ACTOR: {}", &managed_actor.actor_ref.ern());

        managed_actor.acton = acton.clone().unwrap_or_else(|| SystemReady {
            0: ActonInner { broker: managed_actor.actor_ref.broker.clone().unwrap_or_default() },
        });

        managed_actor.ern = managed_actor.actor_ref.ern();

        managed_actor
    }

    #[instrument(skip(self))]
    pub async fn activate(mut self) -> ActorRef {
        let reactors = mem::take(&mut self.reactors);
        let actor_ref = self.actor_ref.clone();

        let active_actor: ManagedActor<Running, ManagedEntity> = self.into();
        let actor = Box::leak(Box::new(active_actor));

        debug_assert!(!actor.inbox.is_closed(), "Actor mailbox is closed in activate");

        let _ = actor_ref.tracker().spawn(actor.wake(reactors));
        actor_ref.tracker().close();

        actor_ref
    }
}

impl<ManagedEntity: Default + Send + Debug + 'static> From<ManagedActor<Idle, ManagedEntity>> for ManagedActor<Running, ManagedEntity> {
    fn from(value: ManagedActor<Idle, ManagedEntity>) -> Self {
        let on_activate = value.on_activate;
        let before_activate = value.before_activate;
        let on_stop = value.on_stop;
        let before_stop = value.before_stop;
        let before_stop_async = value.before_stop_async;
        let halt_signal = value.halt_signal;
        let parent = value.parent;
        let key = value.ern;
        let tracker = value.tracker;
        let acton = value.acton;
        let reactors = value.reactors;

        debug_assert!(
            !value.inbox.is_closed(),
            "Actor mailbox is closed before conversion in From<Actor<Idle, State>>"
        );

        let inbox = value.inbox;
        let actor_ref = value.actor_ref;
        let entity = value.entity;
        let broker = value.broker;

        // tracing::trace!("Mailbox is not closed, proceeding with conversion");
        if actor_ref.children().is_empty() {
            trace!(
                    "child count before Actor creation {}",
                    actor_ref.children().len()
                );
        }
        // Create and return the new actor in the awake state
        ManagedActor::<Running, ManagedEntity>{
            actor_ref,
            parent,
            halt_signal,
            ern: key,
            acton,
            entity,
            tracker,
            inbox,
            before_activate,
            on_activate,
            before_stop,
            on_stop,
            before_stop_async,
            broker,
            reactors,
            _actor_state: Default::default(),
        }
    }
}

impl<ManagedEntity: Default + Send + Debug + 'static> Default for ManagedActor<Idle, ManagedEntity> {
    fn default() -> Self {
        let (outbox, inbox) = channel(255);
        let mut actor_ref: ActorRef = Default::default();
        actor_ref.outbox = outbox.clone();

        ManagedActor::<Idle, ManagedEntity> {
            actor_ref,
            parent: Default::default(),
            ern: Default::default(),
            entity: ManagedEntity::default(),
            broker: Default::default(),
            inbox,
            acton: Default::default(),
            halt_signal: Default::default(),
            tracker: Default::default(),
            before_activate: Box::new(|_| {}),
            on_activate: Box::new(|_| {}),
            before_stop: Box::new(|_| {}),
            on_stop: Box::new(|_| {}),
            before_stop_async: None,
            reactors: DashMap::new(),

            _actor_state: Default::default(),
        }
    }
}


// Function to downcast the message to the original type.
pub fn downcast_message<T: 'static>(msg: &dyn ActonMessage) -> Option<&T> {
    msg.as_any().downcast_ref::<T>()
}
