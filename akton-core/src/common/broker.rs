use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use akton_arn::Arn;

use dashmap::DashMap;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tracing::*;

use crate::actors::{ManagedActor, ActorConfig, Idle};
use crate::common::{Akton, AktonReady, ActorRef};
use crate::message::{BrokerRequest, BrokerRequestEnvelope, SubscribeBroker, UnsubscribeBroker};
use crate::traits::{Actor, AktonMessage};

#[derive(Default, Debug)]
pub struct Broker {
    subscribers: Arc<DashMap<TypeId, HashSet<(String, ActorRef)>>>,
}

impl Broker {
    #[instrument]
    pub(crate) async fn initialize() -> ActorRef {
        let actor_config = ActorConfig::new(Arn::with_root("broker_main").unwrap(), None, None)
            .expect("Couldn't create initial broker config");

        let mut actor = ManagedActor::new(&None, Some(actor_config), Broker::default()).await;

        actor.setup
            .act_on_async::<BrokerRequest>(|actor, event| {
                let subscribers = actor.entity.subscribers.clone();
                let message = event.message.clone();
                let message_type_id = (event.message).message.as_ref().type_id();
                let message_type_name = event.message.message_type_name.clone();
                trace!(message_type_name=message_type_name, message_type_id=?message_type_id);

                Box::pin(async move {
                    Broker::broadcast(subscribers, message).await;
                })
            })
            .act_on_async::<SubscribeBroker>(|actor, event| {
                let message_type_id = event.message.message_type_id;
                let message_type_name = event.message.message_type_name.clone();
                let subscriber_context = event.message.subscriber_context.clone();
                let subscriber_id = event.message.subscriber_id.clone();

                let subscribers = actor.entity.subscribers.clone();
                Box::pin(async move {
                    subscribers
                        .entry(message_type_id)
                        .or_insert_with(HashSet::new)
                        .insert((subscriber_id.clone(), subscriber_context.clone()));
                    trace!(message_type_name=message_type_name, message_type_id=?message_type_id, subscriber=subscriber_context.key, "Subscriber added");
                })
            });

        trace!("Activating the BrokerActor.");
        let mut context = actor.activate().await;
        context.broker = Box::from(Some(context.clone()));
        context
    }

    #[instrument(skip(subscribers))]
    pub async fn broadcast(subscribers: Arc<DashMap<TypeId, HashSet<(String, ActorRef)>>>, request: BrokerRequest) {
        let message_type_id = &request.message.as_ref().type_id();
        if let Some(subscribers) = subscribers.get(&message_type_id) {
            for (_, subscriber_context) in subscribers.value().clone() {
                let subscriber_context = subscriber_context.clone();
                let message: BrokerRequestEnvelope = request.clone().into();
                warn!(subscriber_count=subscribers.len(), type_id=?message_type_id, message=?message, "emitting message");
                subscriber_context.emit(message).await;
            }
        }
    }
}