use std::sync::Arc;
use tracing::{debug, info};
use anyhow::Result;
use petgraph::stable_graph::{NodeIndex, StableGraph};
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use futures::future::{FutureExt, join_all};
use tokio::io::AsyncWriteExt;
use govcraft_actify_macro::govcraft_actor;
use govcraft_actify_core::prelude::*;
// use crate::common::{ActorBuilder, ActorContext, ActorRef, BroadcastContext};
// use govcraft_actify_core::messages::{SystemMessage};
// use crate::traits::actor::Actor;
// use crate::prelude::*;
// use govcraft_actify_core::messages;

type ActorGraph = StableGraph<String, ()>;

pub struct GovcraftSystem {
    system_graph: ActorGraph,
    system_root: NodeIndex,
    system_root_context: Arc<Mutex<SystemRootContext>>,
    user_root_context: Arc<Mutex<UserRootContext>>,
    user_root: NodeIndex,
    group_post: broadcast::Sender<SystemMessage>,
}

#[derive(Debug)]
#[govcraft_actor("SystemMessage")]
pub struct SystemRoot {
    id: String,
}

#[govcraft_async]
impl GovcraftActor for SystemRoot {
    type T = SystemMessage;

    async fn handle_message(&mut self, message: Self::T) -> Result<()> {
        match message {
            SystemMessage::Start => {
                debug!("got start message");
            }
            SystemMessage::Shutdown => {
                self.shutdown().await?;
            }
        }
        Ok(())
    }
}

// impl Actor for SystemRoot {
//     type BroadcastMessage = SystemMessage;
//     type ActorMessage = SystemMessage;
// }

#[derive(Debug)]
#[govcraft_actor("SystemMessage")]
pub struct UserRoot {
    id: String,
}

#[govcraft_async]
impl GovcraftActor for UserRoot {
    type T = SystemMessage;

    async fn handle_message(&mut self, message: Self::T) -> Result<()> {
        Ok(())
    }
}

impl GovcraftSystem {
    pub async fn new() -> Result<GovcraftSystem> {
        let (gms, _gmr) = tokio::sync::broadcast::channel::<SystemMessage>(16);
        //__shutdown: std::sync::atomic::AtomicBool::new(false),
let test = std::sync::atomic::AtomicBool::new(false);
        // let (ams, amr) = tokio::sync::mpsc::channel(16);
        // let (mbs, mbr) = tokio::sync::mpsc::channel(16);
        let system_root_actor = self::SystemRootContext::new(gms.clone(), gms.subscribe(), "hello".to_string()).await;
        // let broadcast_context = Arc::new(Mutex::from(BroadcastContext {
        //     group_mailbox: gms.subscribe(),
        //     group_post: gms.clone(),
        // }));
        // let context = Arc::new(Mutex::from(ActorContext {
        //     // group_mailbox: gms.subscribe(),
        //     admin_mailbox: amr,
        //     admin_post: ams,
        //     direct_post: mbs,
        //     mailbox: mbr,
        // }));
        // let system_root_actor = ActorBuilder::<SystemMessage>::new("govcraft.actify.system_root_actor")
        //     .set_actor(Box::new(system_root_actor))
        //     .set_context(context)
        //     .set_broadcast_context(broadcast_context.clone())
        //     .set_message_handler({
        //         // let context = context.clone();
        //         move |message| {
        //             // let context = context.clone();
        //             async move {
        //                 // let ctx = context.lock().await;
        //                 // ctx.
        //                 match message {
        //                     SystemMessage::Start => {
        //                         info!("Received start message");
        //                     }
        //                     // Handle other messages...
        //                 }
        //                 Ok::<(), anyhow::Error>(())
        //             }.boxed()
        //         }
        //     }
        //     ).build();
        // // let (gms, gmr) = tokio::sync::broadcast::channel(16);
        // let (ams, amr) = tokio::sync::mpsc::channel(16);
        // let (mbs, mbr) = tokio::sync::mpsc::channel(16);
        let user_root_actor = self::UserRootContext::new(gms.clone(), gms.subscribe(), "hello".to_string()).await;
        // let broadcast_context = Arc::new(Mutex::from(BroadcastContext {
        //     group_mailbox: gms.subscribe(),
        //     group_post: gms.clone(),
        // }));
        // let context = Arc::new(Mutex::from(ActorContext {
        //     // group_mailbox: gms.subscribe(),
        //     admin_mailbox: amr,
        //     admin_post: ams,
        //     direct_post: mbs,
        //     mailbox: mbr,
        // }));
        // let user_root_actor = ActorBuilder::new("govcraft.actify.user_root_actor")
        //     .set_actor(Box::new(user_root_actor))
        //     .set_context(context)
        //     .set_broadcast_context(broadcast_context.clone())
        //     .set_message_handler({
        //         // let context = context.clone();
        //         move |message| {
        //             // let context = context.clone();
        //             async move {
        //                 // let ctx = context.lock().await;
        //                 // ctx.
        //                 match message {
        //                     SystemMessage::Start => {
        //                         info!("Starting");
        //                     }
        //                     // Handle other messages...
        //                 }
        //                 Ok::<(), anyhow::Error>(())
        //             }.boxed()
        //         }
        //     }).build();
        //
        let mut system_graph = ActorGraph::new();
        let user_root = system_graph.add_node("user_root_actor.id".to_string());
        let system_root = system_graph.add_node("system_root_actor.id".to_string());
        //

        let system = GovcraftSystem {
            system_graph,
            user_root,
            system_root,
            user_root_context: user_root_actor,
            system_root_context: system_root_actor,
            group_post: gms.clone(),
        };

        Ok(system)
    }
    pub async fn init(&mut self) -> Result<()> {
        // //start the root actors
        // self.start_root_actors().await?;
        // self.start_root_actors().await?;
        //
        // //send a start message
        {
            self.group_post.send(SystemMessage::Start)?;
        }
        Ok(())
    }

    pub async fn await_shutdown(self) -> Result<()> {
        let mut combined_handles = Vec::new();
        {
            let mut system_root_context = self.system_root_context.lock().await;

            debug!("{} system_root_actor handles", system_root_context.actors.len());
            combined_handles.append(&mut system_root_context.actors); // This moves the handles
            let _ = system_root_context.shutdown().await;
        }

        {
            let mut user_root_context = self.user_root_context.lock().await;
            debug!("{} user_root_actor handles", user_root_context.actors.len());
            combined_handles.extend(user_root_context.actors.drain(..)); // This also moves the handles
            let _ = user_root_context.shutdown().await;
        }

        // Now you have a combined vector of all handles
        info!("Joining and awaiting");
        let results = join_all(combined_handles).await;
//
// // Check the results for any errors.
        for result in results {
            info!("awaiting handle");
            // Propagate the error if any of the tasks encountered one.
            match result {
                Ok(_) => (), // Task completed successfully
                Err(e) => return Err(e.into()), // Convert the error as needed and return it
            }
        }

        Ok(())
    }
    async fn start_root_actors(&self) -> Result<()> {
        // let system_root_actor = &self.system_root_actor;
        // let user_root_actor = &self.user_root_actor;
        //
        // let broadcast_context = system_root_actor.broadcast_context.clone();
        // let actor_context = system_root_actor.context.clone();
        // // let actor = system_root_actor.clone();
        // let handle = tokio::spawn(async move {
        //     let actor = system_root_actor.clone();
        //     let actor = actor.actor_ref.as_ref();
        //     actor.pre_run().await?;
        //     actor.run(actor_context, broadcast_context).await?;
        //     Result::<()>::Ok(())
        // });
        // // self.root_actors.push(handle);
        //
        // let broadcast_context = user_root_actor.broadcast_context.clone();
        // let actor_context = user_root_actor.context.clone();
        // let handle = tokio::spawn(async move {
        //     let actor = user_root_actor.clone();
        //     let actor = actor.actor_ref.as_ref();
        //     actor.pre_run().await?;
        //     actor.run(actor_context, broadcast_context).await?;
        //     Result::<()>::Ok(())
        // });
        // self.root_actors.push(handle);
        Ok(())
    }
}
