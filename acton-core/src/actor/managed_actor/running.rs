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

use std::any::type_name_of_val;
use std::fmt::Debug;
use std::time::Duration;

use tokio::time::timeout;
use tracing::{debug, instrument, trace};

use crate::actor::ManagedActor;
use crate::common::{Envelope, OutboundEnvelope, ReactorItem, ReactorMap};
use crate::message::{BrokerRequestEnvelope, ReturnAddress, SystemSignal};
use crate::traits::Actor;

pub struct Running;

impl<ManagedEntity: Default + Send + Debug + 'static> ManagedActor<Running, ManagedEntity> {
    /// Creates a new outbound envelope for the actor.
    ///
    /// # Returns
    /// An optional `OutboundEnvelope` if the context's outbox is available.
    pub fn new_envelope(&self) -> Option<OutboundEnvelope> {
        Option::from(OutboundEnvelope::new(ReturnAddress::new(
            self.actor_ref.outbox.clone(),
            self.ern.clone(),
        )))
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

        while let Some(incoming_envelope) = self.inbox.recv().await {
            let type_id;
            let mut envelope;
            debug!("{}", type_name_of_val(&incoming_envelope.message));
            // Special case for BrokerRequestEnvelope
            if let Some(broker_request_envelope) = incoming_envelope
                .message
                .as_any()
                .downcast_ref::<BrokerRequestEnvelope>()
            {
                envelope = Envelope::new(
                    broker_request_envelope.message.clone(),
                    incoming_envelope.return_address.clone(),
                );
                type_id = broker_request_envelope.message.as_any().type_id().clone();
            } else {
                envelope = incoming_envelope;
                type_id = envelope.message.as_any().type_id().clone();
            }

            if let Some(reactor) = reactors.get(&type_id) {
                match reactor.value() {
                    ReactorItem::MessageReactor(reactor) => (*reactor)(self, &mut envelope),
                    ReactorItem::FutureReactor(fut) => fut(self, &mut envelope).await,
                }
            } else if let Some(SystemSignal::Terminate) =
                envelope.message.as_any().downcast_ref::<SystemSignal>()
            {
                self.terminate().await;
            }
        }
        (self.before_stop)(self);
        if let Some(ref on_before_stop_async) = self.before_stop_async {
            if timeout(Duration::from_secs(5), on_before_stop_async(self))
                .await
                .is_err()
            {
                tracing::error!("on_before_stop_async timed out or failed");
            }
        }
        (self.on_stop)(self);
    }
    #[instrument(skip(self))]
    async fn terminate(&mut self) {
        for item in &self.actor_ref.children() {
            let child_ref = item.value();
            let _ = child_ref.suspend().await;
        }
        trace!(
            actor = self.ern.to_string(),
            "All subordinates terminated. Closing mailbox for"
        );
        self.inbox.close();
    }
}
