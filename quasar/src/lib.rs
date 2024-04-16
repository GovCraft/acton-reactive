use std::any::{Any, TypeId};
use std::io::Write;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;
use crate::traits::{Actor, ActorContext, ActorMessage, IdleActor, IdleState, LifecycleMessage, LifecycleSupervisor};
use std::convert::From;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::broadcast::{channel as BroadcastChannel, Receiver as BroadcastReceiver, Sender as BroadcastSender};
use tokio::task::JoinHandle;
use dashmap::DashMap;
use tokio_util::task::TaskTracker;
use url::{Url, ParseError};
use quasar_qrn::prelude::*;

pub mod traits;

pub struct Quasar<S> {
    pub ctx: S,
}

//region MyActor<MyActorIdle>
impl Quasar<QuasarDormant> {
    pub fn new(domain: &str, category: &str, company: &str, id: &str) -> Quasar<QuasarDormant> {
        let qrn = quasar_qrn::QrnBuilder::new().add::<Domain>(domain).add::<Category>(category).add::<Company>(company).add::<Part>(&*id).build();
        Quasar {
            ctx: QuasarDormant::new(domain, category, company, id)
        }
    }

    // Modified Rust function to avoid the E0499 error by preventing simultaneous mutable borrows of actor.ctx
    pub async fn spawn(actor: Quasar<QuasarDormant>) -> QuasarContext {
        // Ensure the actor is initially in a dormant state
        assert!(matches!(actor.ctx, ref QuasarDormant), "Actor must be dormant to spawn");

        // Convert the actor from MyActorIdle to MyActorRunning
        let mut actor = actor;

        // Handle any pre_start activities
        let pre_start_result = (actor.ctx.on_before_start_reactor)(&actor.ctx);
        assert_eq!(pre_start_result, (), "Pre-start activities failed");

        // Ensure reactors are correctly assigned
        Self::assign_lifecycle_reactors(&mut actor);
        assert!(!actor.ctx.lifecycle_reactor_map.is_empty(), "Lifecycle reactors must be assigned");
        assert!(!actor.ctx.actor_reactor_map.is_empty(), "Actor message reactors must be assigned");

        // Convert to QuasarRunning state
        let mut actor: Quasar<QuasarRunning> = actor.into();
        assert!(matches!(actor.ctx, ref QuasarRunning), "Actor must be in running state after conversion");

        // Take reactor maps and inbox addresses before entering async context
        let lifecycle_message_reactor_map = actor.ctx.lifecycle_message_reactor_map.take().expect("No lifecycle reactors provided. This should never happen");
        let actor_message_reactor_map = actor.ctx.actor_message_reactor_map.take().expect("No actor message reactors provided. This should never happen");

        // Assert that the taken maps are not empty
        assert!(!lifecycle_message_reactor_map.is_empty(), "Lifecycle message reactor map must not be empty");
        assert!(!actor_message_reactor_map.is_empty(), "Actor message reactor map must not be empty");

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


    fn assign_lifecycle_reactors(actor: &mut Quasar<QuasarDormant>) {
        actor.ctx.act_on_lifecycle::<InternalMessage>(|actor, lifecycle_message| {
            match lifecycle_message {
                InternalMessage::Stop => {
                    actor.stop();
                }
            }
        });
    }
}
//endregion


//region Common Types
type LifecycleReactorMap = DashMap<TypeId, LifecycleReactor>;
type LifecycleInbox = Receiver<Box<dyn LifecycleMessage>>;
type LifecycleInboxAddress = Sender<Box<dyn LifecycleMessage>>;
type LifecycleTaskHandle = JoinHandle<()>;

type SupervisorInbox = Option<BroadcastReceiver<Box<dyn ActorMessage>>>;
type SupervisorInboxAddress = Option<BroadcastSender<Box<dyn ActorMessage>>>;

type ActorReactorMap = DashMap<TypeId, ActorReactor>;
type ActorInboxAddress = Sender<Box<dyn ActorMessage>>;
type ActorInbox = Receiver<Box<dyn ActorMessage>>;
type ActorStopFlag = AtomicBool;
type LifecycleStopFlag = AtomicBool;
type ActorTaskHandle = JoinHandle<()>;
//endregion

type ActorChildMap = DashMap<TypeId, ActorReactor>;

type LifecycleEventReactorMut = Box<dyn Fn(&QuasarRunning, &dyn ActorMessage) + Send + Sync>;
type LifecycleEventReactor<T> = Box<dyn Fn(&T) + Send + Sync>;
// type ActorReactor = Box<dyn Fn(&mut MyActorRunning, &dyn ActorMessage) + Send + Sync>;
type LifecycleReactor = Box<dyn Fn(&mut QuasarRunning, &dyn LifecycleMessage) + Send + Sync>;
type AsyncResult<'a> = Pin<Box<dyn Future<Output=()> + Send + 'a>>;
type ActorReactor = Box<dyn Fn(&mut QuasarRunning, &dyn ActorMessage) + Send + Sync>;
//endregion

//region MyActorIdle
pub struct QuasarDormant {
    pub domain: String,
    pub category: String,
    pub company: String,
    pub id: String,
    begin_idle_time: SystemTime,
    on_before_start_reactor: LifecycleEventReactor<Self>,
    on_start_reactor: LifecycleEventReactor<QuasarRunning>,
    on_stop_reactor: LifecycleEventReactor<QuasarRunning>,
    on_before_message_receive_reactor: LifecycleEventReactorMut,
    on_after_message_receive_reactor: LifecycleEventReactor<QuasarRunning>,
    actor_reactor_map: ActorReactorMap,
    lifecycle_reactor_map: LifecycleReactorMap,
}

pub struct QuasarRunning<> {
    pub id: String,
    pub domain: String,
    pub category: String,
    pub company: String,
    supervisor: Option<&'static QuasarRunning>,
    lifecycle_message_reactor_map: Option<LifecycleReactorMap>,
    lifecycle_inbox: LifecycleInbox,
    lifecycle_inbox_address: LifecycleInboxAddress,
    lifecycle_stop_flag: LifecycleStopFlag,
    on_start_reactor: LifecycleEventReactor<QuasarRunning>,
    on_stop_reactor: LifecycleEventReactor<QuasarRunning>,
    on_before_message_receive_reactor: LifecycleEventReactorMut,
    on_after_message_receive_reactor: LifecycleEventReactor<QuasarRunning>,
    actor_message_reactor_map: Option<ActorReactorMap>,
    actor_inbox: ActorInbox,
    actor_inbox_address: ActorInboxAddress,
    actor_stop_flag: ActorStopFlag,
}

impl QuasarDormant {
    //region elapsed time
    pub fn get_elapsed_idle_time_ms(&self) -> Result<u128, String> {
        match SystemTime::now().duration_since(self.begin_idle_time) {
            Ok(duration) => Ok(duration.as_millis()), // Convert the duration to milliseconds
            Err(_) => Err("System time seems to have gone backwards".to_string()),
        }
    }
    //endregion
    pub fn act_on<M: ActorMessage + 'static>(&mut self, actor_message_reactor: impl Fn(&mut QuasarRunning, &M) + Sync + 'static + Send) -> &mut Self {
        // Create a boxed reactor that can be stored in the HashMap.
        let actor_message_reactor_box: ActorReactor = Box::new(move |actor: &mut QuasarRunning, actor_message: &dyn ActorMessage| {
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

    pub fn act_on_lifecycle<M: LifecycleMessage + 'static>(&mut self, lifecycle_message_reactor: impl Fn(&mut QuasarRunning, &M) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        let lifecycle_message_reactor_box: LifecycleReactor = Box::new(move |actor: &mut QuasarRunning, lifecycle_message: &dyn LifecycleMessage| {
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

    pub fn on_before_start(&mut self, life_cycle_event_reactor: impl Fn(&QuasarDormant) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_before_start_reactor = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn on_start(&mut self, life_cycle_event_reactor: impl Fn(&QuasarRunning) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_start_reactor = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn on_stop(&mut self, life_cycle_event_reactor: impl Fn(&QuasarRunning) + Send + Sync + 'static) -> &mut Self {
        // Create a boxed handler that can be stored in the HashMap.
        self.on_stop_reactor = Box::new(life_cycle_event_reactor);
        self
    }

    pub fn new(domain: &str, category: &str, company: &str, id: &str) -> QuasarDormant {
        QuasarDormant {
            domain: "quasar".to_string(),
            category: "system".to_string(),
            company: "govcraft".to_string(),
            id: "root".to_string(),
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

//region impl From<MyActor<MyActorIdle>> for MyActor<MyActorRunning>
impl From<Quasar<QuasarDormant>> for Quasar<QuasarRunning> {
    fn from(value: Quasar<QuasarDormant>) -> Quasar<QuasarRunning> {
        let (actor_inbox_address, actor_inbox) = channel(255);
        let (lifecycle_inbox_address, lifecycle_inbox) = channel(255);

        Quasar {
            ctx: QuasarRunning {
                lifecycle_inbox_address,
                lifecycle_inbox,
                lifecycle_stop_flag: LifecycleStopFlag::new(false),
                on_start_reactor: value.ctx.on_start_reactor,
                on_stop_reactor: value.ctx.on_stop_reactor,
                on_before_message_receive_reactor: value.ctx.on_before_message_receive_reactor,
                on_after_message_receive_reactor: value.ctx.on_after_message_receive_reactor,
                actor_message_reactor_map: Some(value.ctx.actor_reactor_map),
                lifecycle_message_reactor_map: Some(value.ctx.lifecycle_reactor_map),
                actor_inbox,
                actor_inbox_address,
                actor_stop_flag: ActorStopFlag::new(false),
                id: value.ctx.id,
                domain: value.ctx.domain,
                category: value.ctx.category,
                company: value.ctx.company,
                supervisor: None,
            },
        }
    }
}
//endregion

impl IdleState for Quasar<QuasarDormant> {}

//region MyActorRunning


impl QuasarRunning {
    //region actor_listen

    async fn actor_listen(&mut self, actor_message_reactor_map: ActorReactorMap, lifecycle_message_reactor_map: LifecycleReactorMap) {
        let _ = (self.on_start_reactor)(self);
        loop {
            // Fetch and process actor messages if available
            while let Ok(actor_msg) = self.actor_inbox.try_recv() {
                let type_id = actor_msg.as_any().type_id();
                if let Some(reactor) = actor_message_reactor_map.get(&type_id) {
                    {
                        (&self.on_before_message_receive_reactor)(self, &*actor_msg);
                    }
                    reactor(self, &*actor_msg);
                    // (self.on_after_message_receive_reactor)(self);
                } else {
                    eprintln!("No handler for message type: {:?}", actor_msg);
                }
            }

            // Check lifecycle messages
            if let Ok(lifecycle_msg) = self.lifecycle_inbox.try_recv() {
                let type_id = lifecycle_msg.as_any().type_id();
                if let Some(reactor) = lifecycle_message_reactor_map.get(&type_id) {
                    reactor(self, &*lifecycle_msg);
                } else {
                    eprintln!("No handler for message type: {:?}", lifecycle_msg);
                }
            }

            // Check the stop condition after processing messages
            if self.actor_stop_flag.load(Ordering::SeqCst) && self.actor_inbox.is_empty() {
                std::io::stdout().flush().expect("Failed to flush stdout");
                break;
            }
        }
        let _ = (self.on_stop_reactor)(self);
    }

    fn stop(&self) {
        if !self.actor_stop_flag.load(Ordering::SeqCst) {
            self.actor_stop_flag.store(true, Ordering::SeqCst);
        }
    }
}
//endregion

impl IdleActor for Quasar<QuasarDormant> {
    // type State = MyActor<MyActorIdle>;

    fn new() -> Self where Self: Sized {
        // let myactoridle = MyActorIdle::new("".to_string(), "".to_string());

        Quasar::new("quasar", "system", "govcraft", "root")
    }
}

//region MyActorRunning
#[async_trait]
impl Actor for QuasarRunning {
    type Context = QuasarContext;

    fn get_lifecycle_inbox(&mut self) -> &mut LifecycleInbox {
        &mut self.lifecycle_inbox
    }

    fn get_lifecycle_stop_flag(&mut self) -> &mut LifecycleStopFlag {
        &mut self.lifecycle_stop_flag
    }
}
//endregion

#[async_trait]
impl LifecycleSupervisor for QuasarContext {
    fn get_lifecycle_inbox_address(&mut self) -> &mut LifecycleInboxAddress {
        &mut self.lifecycle_inbox_address
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

pub struct QuasarContext
{
    actor_inbox_address: ActorInboxAddress,
    lifecycle_inbox_address: LifecycleInboxAddress,
    task_tracker: TaskTracker,
}

impl QuasarContext {}
//endregion

pub struct Context<A>
    where
        A: Actor<Context=Context<A>>,
{
    phantom: PhantomData<A>,
    // parts: ContextParts<A>,
    // mb: Option<Mailbox<A>>,
}

// Definition of the ActifySystem struct

impl IdleState for QuasarDormant {}

impl Default for Quasar<QuasarDormant> {
    fn default() -> Self {
        Quasar::new("quasar", "system", "govcraft", "root")
    }
}

#[derive(Debug)]
pub enum InternalMessage {
    Stop
}

impl LifecycleMessage for InternalMessage {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

