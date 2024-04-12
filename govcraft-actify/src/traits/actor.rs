use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use anyhow::Result;
use async_trait::async_trait;
use tokio::time::{timeout, Duration};
use tracing::{debug, error, info, instrument, warn};
use govcraft_actify_core::sync::Mutex;
use crate::common::{ActorContext, BroadcastContext};
use crate::messages::{SupervisorMessage, SystemMessage};
use crate::traits::message::GovcraftMessage;

pub type DirectMessageHandler<T> = Arc<dyn FnMut(T) -> Pin<Box<dyn Future<Output=Result<()>> + Send>> + Send + Sync>;

#[async_trait]
pub trait Actor: Send + Sync + Debug {
    type BroadcastMessage: GovcraftMessage + Clone;
    type ActorMessage: GovcraftMessage + Clone;
    async fn handle_supervisor_message(&self, message: SupervisorMessage) -> Result<()> {
        match message {
            SupervisorMessage::Start => {
                info!("Received start message in handler");
            }
        }
        Ok(())
    }
    async fn handle_direct_message(&self, _message: Self::ActorMessage) -> Result<()> {
        info!("Received direct message in handler");

        Ok(())
    }
    async fn handle_broadcast_message(&self, _message: Self::BroadcastMessage) -> Result<()> {
        info!("Received broadcast message in handler");
        Ok(())
    }
    async fn pre_run(&self) -> Result<()> {
        Ok(())
    }
    // #[instrument]
    async fn run(&self, context: Arc<Mutex<ActorContext<Self::ActorMessage>>>, broadcast_context: Arc<Mutex<BroadcastContext<Self::BroadcastMessage>>>) -> anyhow::Result<()> {
        loop {
            info!("looping");

            // Prepare futures for receiving messages from each mailbox.
            let admin_recv_fut = self.recv_admin_message(context.clone());
            let direct_recv_fut = self.recv_direct_message(context.clone());
            let broadcast_recv_fut = self.recv_broadcast_message(broadcast_context.clone());

            tokio::select! {
            Some(msg) = admin_recv_fut => {
                info!("Admin msg");
                // self.handle_supervisor_message(msg).await?;
            },
            Some(msg) = direct_recv_fut => {
                debug!("Direct msg");
                // self.handle_direct_message(msg).await?;
            },
            Ok(msg) = broadcast_recv_fut => {
                info!("Broadcast msg");
                // self.handle_broadcast_message(msg).await?;
            },
            else => {
                info!("else");
                continue;
            },
        }
        }
    }

    #[instrument]
    async fn recv_admin_message(&self, context: Arc<Mutex<ActorContext<<Self as Actor>::ActorMessage>>>) -> Option<SystemMessage> {
        // debug!("locking context");
        let mut locked_context = context.lock().await;
        // debug!("receiving");
        // drop(locked_context);
        locked_context.admin_mailbox.recv().await

    }
    #[instrument]
    async fn recv_direct_message(&self, context: Arc<Mutex<ActorContext<Self::ActorMessage>>>) -> Option<Self::ActorMessage> {
        debug!("locking context for {:?}", self);
        let mut locked_context = context.lock().await;
        // locked_context.mailbox.recv().await
        let recv_result = timeout(Duration::from_secs(1), locked_context.mailbox.recv()).await;
        drop(locked_context);
        match recv_result {
            Ok(Some(msg)) => {
                // handle message
                debug!("message received");
                Some(msg)
            }
            Ok(None) => {
                warn!("no message");
                // channel closed, consider shutting down the actor
                None
            }
            Err(_) => {
                error!("timeout");
                // timeout, consider retrying or shutting down
                None
            }
        }
    }

    async fn recv_broadcast_message(&self, broadcast_context: Arc<Mutex<BroadcastContext<Self::BroadcastMessage>>>) -> Result<Self::BroadcastMessage, tokio::sync::broadcast::error::RecvError> {
        let mut locked_broadcast_context = broadcast_context.lock().await;
        locked_broadcast_context.group_mailbox.recv().await
    }
}


