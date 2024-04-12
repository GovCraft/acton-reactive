use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use anyhow::Result;
use async_trait::async_trait;
use tracing::info;
use govcraft_actify_core::sync::Mutex;
use crate::common::{ActorBuilder, ActorContext, BroadcastContext};
use govcraft_actify_core::prelude::*;
use crate::messages::SupervisorMessage;
use crate::traits::message::GovcraftMessage;

// pub type SupervisorMessageHandler = dyn FnMut(&mut dyn Actor<SupervisorMessage>, SupervisorMessage) -> Result<()> + Send + Sync;
// pub type SafeSupervisorMessageHandler = Arc<Mutex<SupervisorMessageHandler>>;
// pub type DirectMessageHandler<T> = dyn FnMut(T) -> Result<()> + Send;
pub type DirectMessageHandler<T> = Arc<dyn FnMut(T) -> Pin<Box<dyn Future<Output=Result<()>> + Send>> + Send + Sync>;

#[async_trait]
pub trait Actor: Send + Sync {
    type BroadcastMessage: GovcraftMessage;
    type ActorMessage: GovcraftMessage;
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
                self.handle_supervisor_message(msg).await?;
            },
            Some(msg) = direct_recv_fut => {
                info!("Direct msg");
                self.handle_direct_message(msg).await?;
            },
            Ok(msg) = broadcast_recv_fut => {
                info!("Broadcast msg");
                self.handle_broadcast_message(msg).await?;
            },
            else => {
                info!("else");
                continue;
            },
        }
        }
    }

    async fn recv_admin_message(&self, context: Arc<Mutex<ActorContext<<Self as Actor>::ActorMessage>>>) -> Option<SupervisorMessage> {
        let mut locked_context = context.lock().await;
        locked_context.admin_mailbox.recv().await
    }

    async fn recv_direct_message(&self, context: Arc<Mutex<ActorContext<Self::ActorMessage>>>) -> Option<Self::ActorMessage> {
        let mut locked_context = context.lock().await;
        locked_context.mailbox.recv().await
    }

    async fn recv_broadcast_message(&self, broadcast_context: Arc<Mutex<BroadcastContext<Self::BroadcastMessage>>>) -> Result<Self::BroadcastMessage, tokio::sync::broadcast::error::RecvError> {
        let mut locked_broadcast_context = broadcast_context.lock().await;
        locked_broadcast_context.group_mailbox.recv().await
    }
}


#[cfg(test)]
mod tests {
    use crate::common::ActorRef;
    use super::*;


    #[derive(Clone, Debug)]
    enum Msg {
        Hello,
        Alright,
    }

    impl GovcraftMessage for Msg {}

    #[derive(Clone, Debug)]
    enum BroadcastMsg {
        Hello,
        Alright,
    }

    impl GovcraftMessage for BroadcastMsg {}

    #[derive(Clone, Debug)]
    struct MyActor {}

    impl Actor for MyActor {
        type BroadcastMessage = BroadcastMsg;
        type ActorMessage = Msg;
    }


    #[test]
    fn test() {
        // let (_gms, gmr) = tokio::sync::broadcast::channel(16);
        // let (_ams, amr) = tokio::sync::mpsc::channel(16);
        // let (_mbs, mbr) = tokio::sync::mpsc::channel(16);
        // let actor = MyActor {};
        // let context = ActorContext {
        //     group_mailbox: gmr,
        //     admin_mailbox: amr,
        //     mailbox: mbr,
        // };
        // let actor = ActorBuilder::<Msg>::new("my name")
        //     .set_actor(Box::new(actor))
        //     .set_context(context)
        //     .set_message_handler(&move |context, message| -> Result<()> {
        //
        //         Ok(())
        //     }).build();
        //
        //

        assert!(true);
    }
}
