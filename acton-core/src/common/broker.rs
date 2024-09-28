/*
 * Copyright (c) 2024. Govcraft
 *
 * Licensed under either of
 *   * Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *   * MIT license: http://opensource.org/licenses/MIT
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the applicable License for the specific language governing permissions and
 * limitations under that License.
 */

use std::any::{Any, TypeId};
use std::collections::HashSet;
use std::sync::Arc;

use acton_ern::{Ern, UnixTime};
use dashmap::DashMap;
use tracing::*;

use crate::actor::{ActorConfig, Idle, ManagedActor};
use crate::common::ActorRef;
use crate::message::{BrokerRequest, BrokerRequestEnvelope, SubscribeBroker};
use crate::traits::Actor;

#[derive(Default, Debug)]
pub struct Broker {
    subscribers: Arc<DashMap<TypeId, HashSet<(Ern<UnixTime>, ActorRef)>>>,
}

impl Broker {
    #[instrument]
    pub(crate) async fn initialize() -> ActorRef {
        let actor_config = ActorConfig::new(Ern::with_root("broker_main").unwrap(), None, None)
            .expect("Couldn't create initial broker config");

        let mut actor: ManagedActor<Idle, Broker> =
            ManagedActor::new(&None, Some(actor_config)).await;

        actor
            .act_on_async::<BrokerRequest>(|actor, event| {
                let subscribers = actor.entity.subscribers.clone();
                let message = event.message.clone();
                let message_type_id = message.type_id();
                let message_type_name = message.message_type_name.clone();
                debug!(message_type_name=message_type_name, message_type_id=?message_type_id);

                Box::pin(async move {
                    Broker::broadcast(subscribers, message).await;
                })
            })
            .act_on_async::<SubscribeBroker>(|actor, event| {
                let message = event.message.clone();
                let message_type_id = message.message_type_id;
                let subscriber_context = message.subscriber_context.clone();
                let subscriber_id = message.subscriber_id.clone();

                let subscribers = actor.entity.subscribers.clone();
                Box::pin(async move {
                    subscribers
                        .entry(message_type_id)
                        .or_insert_with(HashSet::new)
                        .insert((subscriber_id.clone(), subscriber_context.clone()));
                })
            });

        trace!("Activating the BrokerActor.");
        let mut context = actor.activate().await;
        context.broker = Box::from(Some(context.clone()));
        context
    }

    #[instrument(skip(subscribers))]
    pub async fn broadcast(
        subscribers: Arc<DashMap<TypeId, HashSet<(Ern<UnixTime>, ActorRef)>>>,
        request: BrokerRequest,
    ) {
        let message_type_id = &request.message.as_ref().type_id();
        if let Some(subscribers) = subscribers.get(&message_type_id) {
            for (_, subscriber_context) in subscribers.value().clone() {
                let subscriber_context = subscriber_context.clone();
                let message: BrokerRequestEnvelope = request.clone().into();
                subscriber_context.emit(message).await;
            }
        }
    }
}
