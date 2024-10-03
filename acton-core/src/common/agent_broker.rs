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

use std::any::TypeId;
use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use acton_ern::{Ern, UnixTime};
use dashmap::DashMap;
use futures::future::join_all;
use tracing::*;

use crate::actor::{AgentConfig, Idle, ManagedAgent};
use crate::common::{AgentHandle, BrokerRef};
use crate::message::{BrokerRequest, BrokerRequestEnvelope, SubscribeBroker};
use crate::traits::Actor;

/// A broker that manages subscriptions and broadcasts messages to subscribers.
///
/// The `Broker` struct is responsible for maintaining a list of subscribers for different message types
/// and broadcasting messages to the appropriate subscribers.
#[derive(Default, Debug, Clone)]
pub struct AgentBroker {
    /// A thread-safe map of subscribers, keyed by message type ID.
    ///
    /// Each entry in the map contains a set of tuples, where each tuple consists of:
    /// - An `Ern<UnixTime>`: The unique identifier of the subscriber.
    /// - An `ActorRef`: A reference to the subscriber actor.
    subscribers: Subscribers,
    agent_handle: AgentHandle,
}

type Subscribers = Arc<DashMap<TypeId, HashSet<(Ern<UnixTime>, AgentHandle)>>>; // Type alias for the subscribers map.
// Implement Deref and DerefMut to access AgentHandle's methods directly
impl Deref for AgentBroker {
    type Target = AgentHandle;

    fn deref(&self) -> &Self::Target {
        &self.agent_handle
    }
}

impl DerefMut for AgentBroker {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.agent_handle
    }
}

impl AgentBroker {
    #[instrument]
    pub(crate) async fn initialize() -> BrokerRef {
        let actor_config = AgentConfig::new(Ern::with_root("broker_main").unwrap(), None, None)
            .expect("Couldn't create initial broker config");

        let mut broker: ManagedAgent<Idle, AgentBroker> =
            ManagedAgent::new(&None, Some(actor_config)).await;

        broker
            .act_on::<BrokerRequest>(|actor, event| {
                trace!( "broadcasting request: {:?}", event.message);
                let subscribers = actor.model.subscribers.clone();
                let message = event.message.clone();


                Box::pin(async move {
                    AgentBroker::broadcast(subscribers, message).await;
                })
            })
            .act_on::<SubscribeBroker>(|actor, event| {
                let message = event.message.clone();

                let message_type_id = message.message_type_id;
                let subscriber_context = message.subscriber_context.clone();
                let subscriber_id = message.subscriber_id.clone();
                trace!("subscribe from {} for {}", subscriber_id.root.to_string(), actor.handle.name());

                let subscribers = actor.model.subscribers.clone();
                Box::pin(async move {
                    subscribers
                        .entry(message_type_id)
                        .or_default()
                        .insert((subscriber_id.clone(), subscriber_context.clone()));
                })
            });

        trace!("Activating the BrokerActor.");
        let mut handle = broker.start().await;
        handle.broker = Box::from(Some(handle.clone()));
        handle
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
    pub async fn broadcast(
        subscribers: Subscribers,
        request: BrokerRequest,
    ) {
        let message_type_id = &request.message.as_ref().type_id();
        trace!(" Subscriber count for message type: {:?} is {:?}", message_type_id, subscribers.get(message_type_id).map(|x| x.len()));
        if let Some(subscribers) = subscribers.get(message_type_id) {
            let futures = subscribers.value().clone().into_iter().map(|(_, subscriber_context)| {
                let subscriber_context = subscriber_context.clone();
                let message: BrokerRequestEnvelope = request.clone().into();
                async move {
                    trace!("Broadcasting message to subscriber: {:?}", subscriber_context.name());
                    subscriber_context.send(message).await;
                }
            });
            // Await all futures concurrently
            join_all(futures).await;
        }
    }
}
