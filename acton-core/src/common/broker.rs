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

/// A broker that manages subscriptions and broadcasts messages to subscribers.
///
/// The `Broker` struct is responsible for maintaining a list of subscribers for different message types
/// and broadcasting messages to the appropriate subscribers.
#[derive(Default, Debug)]
pub struct Broker {
    /// A thread-safe map of subscribers, keyed by message type ID.
    ///
    /// Each entry in the map contains a set of tuples, where each tuple consists of:
    /// - An `Ern<UnixTime>`: The unique identifier of the subscriber.
    /// - An `ActorRef`: A reference to the subscriber actor.
    subscribers: Subscribers,
}
type Subscribers = Arc<DashMap<TypeId, HashSet<(Ern<UnixTime>, ActorRef)>>>; // Type alias for the subscribers map.

impl Broker {
    #[instrument]
    pub(crate) async fn initialize() -> ActorRef {
        let actor_config = ActorConfig::new(Ern::with_root("broker_main").unwrap(), None, None)
            .expect("Couldn't create initial broker config");

        let mut actor: ManagedActor<Idle, Broker> =
            ManagedActor::new(&None, Some(actor_config)).await;

        actor
            .act_on::<BrokerRequest>(|actor, event| {
                let subscribers = actor.entity.subscribers.clone();
                let message = event.message.clone();
                let message_type_id = message.type_id();
                let message_type_name = message.message_type_name.clone();
                debug!(message_type_name=message_type_name, message_type_id=?message_type_id);

                Box::pin(async move {
                    Broker::broadcast(subscribers, message).await;
                })
            })
            .act_on::<SubscribeBroker>(|actor, event| {
                let message = event.message.clone();
                let message_type_id = message.message_type_id;
                let subscriber_context = message.subscriber_context.clone();
                let subscriber_id = message.subscriber_id.clone();

                let subscribers = actor.entity.subscribers.clone();
                Box::pin(async move {
                    subscribers
                        .entry(message_type_id)
                        .or_default()
                        .insert((subscriber_id.clone(), subscriber_context.clone()));
                })
            });

        trace!("Activating the BrokerActor.");
        let mut context = actor.start().await;
        context.broker = Box::from(Some(context.clone()));
        context
    }

    /// Broadcasts a message to all subscribers of a specific message type.
    ///
    /// This function iterates through all subscribers for the given message type and emits
    /// the message to each subscriber asynchronously.
    ///
    /// # Arguments
    ///
    /// * `subscribers` - An `Arc<DashMap>` containing the subscribers for different message types.
    /// * `request` - The `BrokerRequest` containing the message to be broadcast.
    /// ```
    #[instrument(skip(subscribers))]
    pub async fn broadcast(
        subscribers: Subscribers,
        request: BrokerRequest,
    ) {
        let message_type_id = &request.message.as_ref().type_id();
        if let Some(subscribers) = subscribers.get(message_type_id) {
            for (_, subscriber_context) in subscribers.value().clone() {
                let subscriber_context = subscriber_context.clone();
                let message: BrokerRequestEnvelope = request.clone().into();
                subscriber_context.emit(message).await;
            }
        }
    }
}
