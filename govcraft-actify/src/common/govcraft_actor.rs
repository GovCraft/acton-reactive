use std::sync::Arc;
use govcraft_actify_core::sync::Mutex;
use crate::common::{ActorContext, ActorRef};
use crate::common::actor_context::BroadcastContext;
use crate::traits::actor::DirectMessageHandler;
use crate::traits::message::GovcraftMessage;

pub struct GovcraftActor<T> where T: GovcraftMessage {
    pub id: &'static str,
    pub actor_ref: Arc<Mutex<ActorRef<T>>>,
    pub context: Arc<Mutex<ActorContext<T>>>,
    pub broadcast_context: Arc<Mutex<BroadcastContext<T>>>,
    pub(crate) message_handler: DirectMessageHandler<T>,

}


