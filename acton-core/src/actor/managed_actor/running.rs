/*
 *
 *  *
 *  * Copyright (c) 2024 Govcraft.
 *  *
 *  *  Licensed under the Business Source License, Version 1.1 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *      https://github.com/GovCraft/acton-framework/tree/main/LICENSES
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

use std::any::type_name_of_val;
use std::fmt::Debug;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, instrument, trace};
use crate::actor::ManagedActor;
use crate::common::{Envelope, OutboundEnvelope, ReactorItem, ReactorMap};
use crate::message::{BrokerRequestEnvelope, SystemSignal};
use crate::traits::Actor;

pub struct Running;

impl<ManagedEntity: Default + Send + Debug + 'static> ManagedActor<Running, ManagedEntity> {
    /// Creates a new outbound envelope for the actor.
    ///
    /// # Returns
    /// An optional `OutboundEnvelope` if the context's outbox is available.
    pub fn new_envelope(&self) -> Option<OutboundEnvelope> {
        if let Some(envelope) = &self.actor_ref.outbox {
            Option::from(OutboundEnvelope::new(
                Some(envelope.clone()),
                self.key.clone(),
            ))
        } else {
            None
        }
    }

    /// Creates a new parent envelope for the actor.
    ///
    /// # Returns
    /// A clone of the parent's return envelope.
    pub fn new_parent_envelope(&self) -> Option<OutboundEnvelope> {
        if let Some(parent) = &self.parent {
            Some(parent.return_address().clone())
        } else {
            None
        }
    }

    #[instrument(skip(reactors, self))]
    pub(crate) async fn wake(&mut self, reactors: ReactorMap<ManagedEntity>) {
        (self.on_activate)(self);

        while let Some(mut incoming_envelope) = self.inbox.recv().await {
            let type_id;
            let mut envelope;
            debug!("{}",type_name_of_val(&incoming_envelope.message));
            // Special case for BrokerRequestEnvelope
            if let Some(broker_request_envelope) = incoming_envelope.message.as_any().downcast_ref::<BrokerRequestEnvelope>() {
                envelope = Envelope::new(
                    broker_request_envelope.message.clone(),
                    incoming_envelope.return_address.clone(),
                );
                type_id = broker_request_envelope.message.as_any().type_id().clone();
            } else {
                envelope = incoming_envelope;
                type_id = envelope.message.as_any().type_id().clone();
            }
            let is_target = reactors.len() > 0;

            if let Some(reactor) = reactors.get(&type_id) {
                match reactor.value() {
                    ReactorItem::MessageReactor(reactor) => (*reactor)(self, &mut envelope),
                    ReactorItem::FutureReactor(fut) => fut(self, &mut envelope).await,
                    _ => tracing::warn!("Unknown ReactorItem type for: {:?}", &type_id.clone()),
                }
            } else if let Some(SystemSignal::Terminate) = envelope.message.as_any().downcast_ref::<SystemSignal>() {
                trace!(actor=self.key, "Mailbox received {:?} with type_id {:?} for", &envelope.message, &type_id);
                self.terminate().await;
            }
        }
        (self.before_stop)(self);
        if let Some(ref on_before_stop_async) = self.before_stop_async {
            if timeout(Duration::from_secs(5), on_before_stop_async(self)).await.is_err() {
                tracing::error!("on_before_stop_async timed out or failed");
            }
        }
        (self.on_stop)(self);
    }
    #[instrument(skip(self))]
    async fn terminate(&mut self) {
        tracing::trace!(actor=self.key, "Received SystemSignal::Terminate for");
        for item in &self.actor_ref.children() {
            let child_ref = item.value();
            let _ = child_ref.suspend().await;
        }
        trace!(actor=self.key,"All subordinates terminated. Closing mailbox for");
        self.inbox.close();
    }
}


