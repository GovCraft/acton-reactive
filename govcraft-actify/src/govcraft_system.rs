#![allow(dead_code)]

use std::sync::Arc;
use tracing::{error, info, warn};
use anyhow::Result;
use petgraph::stable_graph::StableGraph;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use futures::future::join_all;
use govcraft_actify_macro::govcraft_actor;
use govcraft_actify_core::prelude::*;

type ActorGraph = StableGraph<String, ()>;

pub struct GovcraftSystem {
    // system_graph: ActorGraph,
    // system_root: NodeIndex,
    system_root_context: Arc<Mutex<SystemRootContext>>,
    user_root_context: Arc<Mutex<UserRootContext>>,
    // user_root: NodeIndex,
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
                info!("actor {} received Start from System", self.id);
            }
            SystemMessage::Shutdown => {
                warn!("actor {} received Shutdown from System", self.id);
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug)]
#[govcraft_actor("SystemMessage")]
pub struct UserRoot {
    id: String,
}

#[govcraft_async]
impl GovcraftActor for UserRoot {
    type T = SystemMessage;
    async fn handle_message(&mut self, message: Self::T) -> Result<()> {
        match message {
            SystemMessage::Start => {
                tracing::trace!("actor {} received Start from System", self.id);
            }
            SystemMessage::Shutdown => {
                tracing::trace!("actor {} received Shutdown from System", self.id);
            }
            _ => {}
        }
        Ok(())
    }
}

impl GovcraftSystem {
    pub async fn new() -> Result<GovcraftSystem> {
        let (gms, _gmr) = tokio::sync::broadcast::channel::<SystemMessage>(16);

        let system_root_actor = SystemRootContext::init(gms.clone(), gms.subscribe(), "govcraft::actify::system::system_root".to_string()).await;
        let user_root_actor = UserRootContext::init(gms.clone(), gms.subscribe(), "govcraft::actify::system::user_root".to_string()).await;

        let mut system_graph = ActorGraph::new();
        let _user_root = system_graph.add_node("user_root_actor.id".to_string());
        let _system_root = system_graph.add_node("system_root_actor.id".to_string());

        let system = GovcraftSystem {
            // system_graph,
            // user_root,
            // system_root,
            user_root_context: user_root_actor,
            system_root_context: system_root_actor,
            group_post: gms.clone(),
        };

        Ok(system)
    }

    pub async fn await_shutdown(self) -> Result<()> {
        let mut combined_handles = Vec::new();
        {
            let mut system_root_context = self.system_root_context.lock().await;

            // trace!("{} system_root_actor handles", system_root_context.actors.len());
            combined_handles.append(&mut system_root_context.actors); // This moves the handles
            let _ = system_root_context.shutdown().await;
        }

        {
            let mut user_root_context = self.user_root_context.lock().await;
            // trace!("{} user_root_actor handles", user_root_context.actors.len());
            combined_handles.append(&mut user_root_context.actors); // This also moves the handles
            let _ = user_root_context.shutdown().await;
        }

        // Now you have a combined vector of all handles
        let results = join_all(combined_handles).await;

        for result in results {
            // Propagate the error if any of the tasks encountered one.
            match result {
                Ok(_) => {} // Task completed successfully
                Err(e) => {
                    error!("{:?}", e);
                    return Err(e.into());
                } // Convert the error as needed and return it
            }
        }

        Ok(())
    }
}
