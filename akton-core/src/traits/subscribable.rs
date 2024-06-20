/*
 *
 *  *
 *  * Copyright (c) 2024 Govcraft.
 *  *
 *  *  Licensed under the Business Source License, Version 1.1 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *      https://github.com/GovCraft/akton-framework/tree/main/LICENSES
 *  *
 *  *  Change Date: Three years from the release date of this version of the Licensed Work.
 *  *  Change License: Apache License, Version 2.0
 *  *
 *  *  Usage Limitations:
 *  *    - You may use the Licensed Work for non-production purposes only, such as internal testing, development, and experimentation.
 *  *    - You may not use the Licensed Work for any production or commercial purpose, including, but not limited to, the provision of any service to third parties, without a commercial use license from the Licensor, except as stated in the Exemptions section of the License.
 *  *
 *  *  Exemptions:
 *  *    - Open Source Projects licensed under an OSI-approved open source license.
 *  *    - Non-Profit Organizations using the Licensed Work for non-commercial purposes.
 *  *    - Small For-Profit Companies with annual gross revenues not exceeding $2,000,000 USD.
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */

use std::any::TypeId;
use std::future::Future;
use async_trait::async_trait;
use tracing::*;
use crate::message::{SubscribeBroker, UnsubscribeBroker};
use crate::traits::{ActorContext, AktonMessage};
use crate::traits::subscriber::Subscriber;
use dyn_clone::DynClone;
#[async_trait]
pub trait Subscribable {
    fn subscribe<T: AktonMessage + Send + Sync + 'static>(&self) -> impl Future<Output=()> + Send + Sync + '_
    where

        Self: ActorContext + Subscriber;
    fn unsubscribe<T: AktonMessage>(&self)
    where

        Self: ActorContext + Subscriber + Send + Sync + 'static;
}

#[async_trait]
impl<T> Subscribable for T
where
    T: AktonMessage + Send + Sync + 'static,
{
    fn subscribe<M: AktonMessage + Send + Sync + 'static>(&self) -> impl Future<Output=()> + Send + Sync + '_
    where
        Self: ActorContext + Subscriber + 'static,
    {
        let subscriber_id = self.key();
        let subscription = SubscribeBroker {
            subscriber_id,
            message_type_id: TypeId::of::<M>(),
            subscriber_context: self.clone_self(),
        };
        let broker = self.broker();
        let key = self.key().clone();

        async move {
            if let Some(broker) = broker {
                let broker_key = broker.key();
                debug!(
                          type_id=?TypeId::of::<M>(),
                          subscribing_actor = key,
                          "Subscribing to type_name {} with broker {}",
                          std::any::type_name::<M>(),
                          broker_key
                      );
                broker.emit_async(subscription, None).await;
            }
        }
    }
    fn unsubscribe<M: AktonMessage>(&self)
    where
        Self: ActorContext + Subscriber,
    {
        let subscriber_id = self.key();
        let subscription = UnsubscribeBroker {
            subscriber_id,
            message_type_id: TypeId::of::<M>(),
            subscriber_context: self.clone_self(),
        };
        let broker = self.broker();
        if let Some(broker) = broker {
            let broker = broker.clone();
            tokio::spawn(async move {
                broker.emit_async(subscription, None).await;
            });
        }
        trace!(
            type_id = ?TypeId::of::<M>(),
            repository_actor = self.key(),
            "Unsubscribed to {}",
            std::any::type_name::<M>()
        );
    }
}
