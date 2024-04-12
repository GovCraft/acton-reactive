use std::sync::Arc;
use tracing::{debug, info};
use anyhow::Result;
use petgraph::stable_graph::{NodeIndex, StableGraph};
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use futures::future::{FutureExt, join_all};
use crate::common::{ActorBuilder, ActorContext, ActorRef, BroadcastContext, GovcraftActor};
use crate::messages::{SystemMessage};
use crate::traits::actor::Actor;

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

#[derive(Debug, Clone)]
pub struct SystemRoot;

impl Actor for SystemRoot {
    type BroadcastMessage = SystemMessage;
    type ActorMessage = SystemMessage;
}

#[derive(Debug, Clone)]
pub struct UserRoot;

impl Actor for UserRoot {
    type BroadcastMessage = SystemMessage;
    type ActorMessage = SystemMessage;
}

impl<'a> GovcraftSystem<'a> {
    pub async fn new() -> Result<GovcraftSystem<'a>> {

        let (gms, _gmr) = tokio::sync::broadcast::channel(16);
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


        let system = GovcraftSystem {
            system_graph,
            user_root,
            system_root,
            user_root_actor,
            system_root_actor,
            root_actors: vec![],
            group_post: gms,
        };

        Ok(system)
    }
    pub async fn init(&mut self) -> Result<()> {
        //start the root actors
            self.start_actor(self.user_root_actor.actor_ref.clone(), self.user_root_actor.context.clone(), self.user_root_actor.broadcast_context.clone()).await?;
            self.start_actor(self.system_root_actor.actor_ref.clone(), self.system_root_actor.context.clone(), self.system_root_actor.broadcast_context.clone()).await?;
        //send a start message

        {
            let root = self.system_root_actor.context.lock().await;
            root.direct_post.send(SystemMessage::Start).await?;
        }
        Ok(())
    }

    pub async fn await_shutdown(self) -> Result<()> {
        // Collect all root actor tasks into a Vec<JoinHandle<Result<()>>>
        let root_actor_handles = self.root_actors;
        debug!("handle count: {}", root_actor_handles.len());
        // Use join_all to wait for all tasks to complete.
        // join_all returns a future that resolves to a Vec<Result<()>> once all futures resolve.
        let results = join_all(root_actor_handles).await;

        // Check the results for any errors.
        for result in results {
            // Propagate the error if any of the tasks encountered one.
            result??; // Double `?` to propagate errors: the outer from JoinHandle, the inner from your task result.
        }

        Ok(())
    }
    async fn start_actor(&mut self, actor_ref: Arc<Mutex<ActorRef<SystemMessage>>>, actor_context: Arc<Mutex<ActorContext<SystemMessage>>>, broadcast_context: Arc<Mutex<BroadcastContext<SystemMessage>>>) -> Result<()>{
        let actor = actor_ref.lock().await.actor.clone();
        let broadcast_context = broadcast_context.clone();
        let handle = tokio::spawn(async move {
            let actor = actor.lock().await;
            actor.pre_run().await?;
            actor.run(actor_context, broadcast_context).await?;
            Result::<()>::Ok(())
        });
        self.root_actors.push(handle);
        Ok(())
    }
}
