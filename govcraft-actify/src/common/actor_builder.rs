use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use anyhow::Result;
// use tracing::info;
use govcraft_actify_core::sync::Mutex;
use crate::common::{ActorContext, ActorRef, GovcraftActor};
use crate::common::actor_context::BroadcastContext;
// use crate::govcraft_system::{SystemRoot, UserRoot};
// use crate::messages::{SupervisorMessage, SystemMessage};
use crate::traits::actor::{Actor, DirectMessageHandler};
use crate::traits::message::GovcraftMessage;

pub struct ActorBuilder<T> where T: GovcraftMessage {
    id: &'static str,
    message_handler: Option<DirectMessageHandler<T>>,
    context: Option<Arc<Mutex<ActorContext<T>>>>,
    broadcast_context: Option<Arc<Mutex<BroadcastContext<T>>>>,
    actor: Option<Box<dyn Actor<ActorMessage=T, BroadcastMessage=T>>>,

}


impl<T: GovcraftMessage> ActorBuilder<T> {
    pub(crate) fn new(id: &'static str) -> ActorBuilder<T> where Self: Sized {
        ActorBuilder { id, message_handler: None, context: None, actor: None, broadcast_context: None }
    }
    pub fn build(self) -> GovcraftActor<T> {
        GovcraftActor {
            id: self.id,
            actor_ref: self.actor.expect("No actor provided"),
            context: self.context.expect("No context provided"),
            message_handler: self.message_handler.expect("no message handler provided"),
            broadcast_context: self.broadcast_context.expect("no broadcast context"),
        }
    }
    pub fn set_context(mut self, context: Arc<Mutex<ActorContext<T>>>) -> ActorBuilder<T> {
        self.context = Some(context);
        self
    }
    pub fn set_broadcast_context(mut self, context: Arc<Mutex<BroadcastContext<T>>>) -> ActorBuilder<T> {
        self.broadcast_context = Some(context);
        self
    }
    pub fn set_message_handler<F>(mut self, message_handler: F) -> ActorBuilder<T>
        where F: FnMut(T) -> Pin<Box<dyn Future<Output=Result<()>> + Send>> + Send + Sync + 'static {
        self.message_handler = Some(Arc::new(message_handler));
        self
    }
    pub fn set_actor(mut self, actor: Box<dyn Actor<ActorMessage=T, BroadcastMessage=T>>) -> ActorBuilder<T> {
        let actor_ref =actor;
        self.actor = Some(actor_ref);
        self
    }
}
