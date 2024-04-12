use std::sync::{Arc};
use tokio::sync::Mutex;
use async_trait::async_trait;
use crate::common::ActorContext;
use crate::traits::actor::Actor;
use crate::traits::message::GovcraftMessage;

pub struct AnyActor<T>(dyn Actor<ActorMessage=T, BroadcastMessage=T>);

unsafe impl<T> Send for AnyActor<T> {
}

pub struct ActorRef<T> where T: GovcraftMessage {
    pub actor: Arc<Mutex<dyn Actor<ActorMessage=T, BroadcastMessage=T>>>,
}

unsafe impl<T:GovcraftMessage> Send for ActorRef<T> {}

