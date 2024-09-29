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
use std::future::Future;

use async_trait::async_trait;
use tracing::*;

use crate::message::{SubscribeBroker, UnsubscribeBroker};
use crate::traits::{ActonMessage, Actor, Broker};
use crate::traits::subscriber::Subscriber;

/// Trait for types that can subscribe to and unsubscribe from messages.
#[async_trait]
pub trait Subscribable {
    /// Subscribes the implementing type to messages of type `T`.
    ///
    /// # Type Parameters
    ///
    /// * `T`: The type of message to subscribe to. Must implement `ActonMessage + Send + Sync + 'static`.
    ///
    /// # Returns
    ///
    /// A `Future` that resolves to `()` when the subscription is complete.
    fn subscribe<T: ActonMessage + Send + Sync + 'static>(
        &self,
    ) -> impl Future<Output=()> + Send + Sync + '_
    where
        Self: Actor + Subscriber;

    /// Unsubscribes the implementing type from messages of type `T`.
    ///
    /// # Type Parameters
    ///
    /// * `T`: The type of message to unsubscribe from. Must implement `ActonMessage`.
    fn unsubscribe<T: ActonMessage>(&self)
    where
        Self: Actor + Subscriber + Send + Sync + 'static;
}

/// Implementation of `Subscribable` for any type that implements `ActonMessage + Send + Sync + 'static`.
#[async_trait]
impl<T> Subscribable for T
where
    T: ActonMessage + Send + Sync + 'static,
{
    fn subscribe<M: ActonMessage + Send + Sync + 'static>(
        &self,
    ) -> impl Future<Output=()> + Send + Sync + '_
    where
        Self: Actor + Subscriber + 'static,
    {
        let subscriber_id = self.id();
        let message_type_id = TypeId::of::<M>();
        let message_type_name = std::any::type_name::<M>().to_string();
        let subscription = SubscribeBroker {
            subscriber_id,
            message_type_id,
            subscriber_context: self.clone_ref(),
        };
        let broker = self.get_broker();
        let ern = self.id().clone();

        async move {
            debug!( type_id=?message_type_id, subscriber_ern = ern.to_string(), "Subscribing to type_name {}", message_type_name);
            if let Some(emit_broker) = broker {
                let broker_key = emit_broker.id();
                debug!(
                    type_id=?message_type_id,
                    subscriber_ern = ern.to_string(),
                    "Subscribing to type_name {} with broker {}",
                    message_type_name,
                    broker_key
                );
                emit_broker.send_message(subscription).await;
            } else {
                tracing::error!( type_id=?message_type_id, subscriber_ern = ern.to_string(), "No broker found for type_name {}", message_type_name);
            }
        }
    }
    fn unsubscribe<M: ActonMessage>(&self)
    where
        Self: Actor + Subscriber,
    {
        // let subscriber_id = self.ern();
        let subscription = UnsubscribeBroker {
            // ern: subscriber_id,
            // message_type_id: TypeId::of::<M>(),
            // subscriber_ref: self.clone_ref(),
        };
        let broker = self.get_broker();
        if let Some(broker) = broker {
            let broker = broker.clone();
            tokio::spawn(async move {
                broker.send_message(subscription).await;
            });
        }
        trace!(
            type_id = ?TypeId::of::<M>(),
            repository_actor = self.id().to_string(),
            "Unsubscribed to {}",
            std::any::type_name::<M>()
        );
    }
}
