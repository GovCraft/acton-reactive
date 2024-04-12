use std::ffi::FromBytesUntilNulError;
use std::marker::PhantomData;
use std::sync::Arc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use anyhow::Result;
use tracing_subscriber::util::SubscriberInitExt;
use petgraph::stable_graph::{NodeIndex, StableGraph};
use petgraph::Direction;
use petgraph::graph::Node;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use futures::future::FutureExt;
// Make sure this is included
use crate::common::{ActorBuilder, ActorContext, ActorRef, BroadcastContext, GovcraftActor};
use crate::messages::{SupervisorMessage, SystemMessage};
use crate::traits::actor::Actor;
use crate::traits::message::GovcraftMessage;

// Type alias for simplicity
// type ActorGraph<'a, T: GovcraftMessage, B: GovcraftMessage> = StableGraph<GovcraftActor<'a, T, B>, ()>;
type ActorGraph<'a> = StableGraph<&'a str, ()>;

pub struct GovcraftSystem<'a> {
    system_graph: ActorGraph<'a>,
    system_root: NodeIndex,
    system_root_actor: GovcraftActor<SystemMessage>,
    user_root_actor: GovcraftActor<SystemMessage>,
    user_root: NodeIndex,
    root_actors: Vec<JoinHandle<core::result::Result<(), anyhow::Error>>>,
    group_post: broadcast::Sender<SystemMessage>,
}

pub struct SystemRoot;

impl Actor for SystemRoot {
    type BroadcastMessage = SystemMessage;
    type ActorMessage = SystemMessage;
}

pub struct UserRoot;

impl Actor for UserRoot {
    type BroadcastMessage = SystemMessage;
    type ActorMessage = SystemMessage;
}

impl<'a> GovcraftSystem<'a> {
    pub async fn new() -> Result<GovcraftSystem<'a>> {

        let (gms, gmr) = tokio::sync::broadcast::channel(16);
        let (ams, amr) = tokio::sync::mpsc::channel(16);
        let (mbs, mbr) = tokio::sync::mpsc::channel(16);
        let system_root_actor = Arc::new(Mutex::from(SystemRoot));
        let broadcast_context = Arc::new(Mutex::from(BroadcastContext {
            group_mailbox: gms.subscribe(),
            group_post: gms.clone(),
        }));
        let context =Arc::new(Mutex::from(ActorContext {
            // group_mailbox: gms.subscribe(),
            admin_mailbox: amr,
            admin_post: ams,
            direct_post: mbs,
            mailbox: mbr,
        }));
        let system_root_actor = ActorBuilder::<SystemMessage>::new("govcraft.actify.system_root_actor")
            .set_actor(system_root_actor.clone())
            .set_context(context)
            .set_broadcast_context(broadcast_context.clone())
            .set_message_handler({
                // let context = context.clone();
                move |message| {
                    // let context = context.clone();
                    async move {
                        // let ctx = context.lock().await;
                        // ctx.
                        match message {
                            SystemMessage::Start => {
                                info!("Received start message");
                            }
                            // Handle other messages...
                        }
                        Ok::<(), anyhow::Error>(())
                    }.boxed()
                }
            }
            ).build();
        // let (gms, gmr) = tokio::sync::broadcast::channel(16);
        let (ams, amr) = tokio::sync::mpsc::channel(16);
        let (mbs, mbr) = tokio::sync::mpsc::channel(16);
        let user_root_actor = Arc::new(Mutex::from(UserRoot));
        let broadcast_context = Arc::new(Mutex::from(BroadcastContext {
            group_mailbox: gms.subscribe(),
            group_post: gms.clone(),
        }));
        let context =Arc::new(Mutex::from(ActorContext {
            // group_mailbox: gms.subscribe(),
            admin_mailbox: amr,
            admin_post: ams,
            direct_post: mbs,
            mailbox: mbr,
        }));
        let user_root_actor = ActorBuilder::new("govcraft.actify.user_root_actor")
            .set_actor(user_root_actor.clone())
            .set_context(context)
            .set_broadcast_context(broadcast_context.clone())
            .set_message_handler({
                // let context = context.clone();
                move |message| {
                    // let context = context.clone();
                    async move {
                        // let ctx = context.lock().await;
                        // ctx.
                        match message {
                            SystemMessage::Start => {
                                info!("Starting");
                            }
                            // Handle other messages...
                        }
                        Ok::<(), anyhow::Error>(())
                    }.boxed()
                }
            }).build();

        let mut system_graph = ActorGraph::new();
        let user_root = system_graph.add_node(user_root_actor.id);
        let system_root = system_graph.add_node(system_root_actor.id);

// let actor = user_root_actor.actor_ref.lock().await.actor.clone();
// let user_root_handle = tokio::spawn(async move {
//     let actor = actor.lock().await;
//     actor.pre_run().await.expect("Could not execute actor pre_run");
//     actor.run(context.clone(), broadcast_context.clone()).await.expect("Could not execute actor run");
// });

        let system = GovcraftSystem {
            system_graph,
            user_root,
            system_root,
            user_root_actor,
            system_root_actor,
            root_actors: vec![],
            group_post: gms,
        };
// system.init()?;

        Ok(system)
    }
    pub async fn init(&mut self) -> Result<()> {
        //start the root actors
            self.start_actor(self.user_root_actor.actor_ref.clone(), self.user_root_actor.context.clone(), self.user_root_actor.broadcast_context.clone()).await?;
            self.start_actor(self.system_root_actor.actor_ref.clone(), self.system_root_actor.context.clone(), self.system_root_actor.broadcast_context.clone()).await?;
        //send a start message
        let root = self.system_root_actor.context.lock().await;
        root.direct_post.send(SystemMessage::Start).await?;

        Ok(())
    }

    async fn start_actor(&mut self, actor_ref: Arc<Mutex<ActorRef<SystemMessage>>>, actor_context: Arc<Mutex<ActorContext<SystemMessage>>>, broadcast_context: Arc<Mutex<BroadcastContext<SystemMessage>>>) -> Result<()>{
        let actor = actor_ref.lock().await.actor.clone();
        let broadcast_context = broadcast_context.clone();
        let handle = tokio::spawn(async move {
            let actor = actor.lock().await;
            actor.pre_run().await?;
            actor.run(actor_context, broadcast_context).await?;
            anyhow::Result::<()>::Ok(())
        });
        self.root_actors.push(handle);
        Ok(())
    }
}