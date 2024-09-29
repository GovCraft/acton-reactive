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

use futures::future::join_all;
use tokio::time::timeout;
use tracing::{debug, instrument, trace};

use crate::actor::ManagedAgent;
use crate::common::{Envelope, OutboundEnvelope, ReactorItem, ReactorMap};
use crate::message::{BrokerRequestEnvelope, ReturnAddress, SystemSignal};
use crate::traits::Actor;

pub struct Started;

impl<Agent: Default + Send + Debug + 'static> ManagedAgent<Started, Agent> {
    /// Creates a new outbound envelope for the actor.
    ///
    /// # Returns
    /// An optional `OutboundEnvelope` if the context's outbox is available.
    pub fn new_envelope(&self) -> Option<OutboundEnvelope> {
        Option::from(OutboundEnvelope::new(ReturnAddress::new(
            self.handle.outbox.clone(),
            self.id.clone(),
        )))
    }

    /// Creates a new parent envelope for the actor.
    ///
    /// # Returns
    /// A clone of the parent's return envelope.
    pub fn new_parent_envelope(&self) -> Option<OutboundEnvelope> {
        self.parent.as_ref().map(|parent| parent.return_address().clone())
    }

    #[instrument(skip(reactors, self))]
    pub(crate) async fn wake(&mut self, reactors: ReactorMap<Agent>) {
        (self.after_start)(self).await;
        let mut terminate_requested = false;
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
                type_id = broker_request_envelope.message.as_any().type_id();
            } else {
                envelope = incoming_envelope;
                type_id = envelope.message.as_any().type_id();
            }

            if let Some(reactor) = reactors.get(&type_id) {
                match reactor.value() {
                    ReactorItem::MessageReactor(reactor) => (*reactor)(self, &mut envelope),
                    ReactorItem::FutureReactor(fut) => fut(self, &mut envelope).await,
                }
            } else if let Some(SystemSignal::Terminate) =
                envelope.message.as_any().downcast_ref::<SystemSignal>()
            {
                // Set the termination flag
                terminate_requested = true;
                debug!("Termination signal received, waiting for remaining messages...");
                (self.before_stop)(self).await;
                self.inbox.close();
                break;
            }
        }
        loop {
            // Check if termination has been requested and the inbox is empty
            if terminate_requested && self.inbox.is_empty() && self.inbox.is_closed() {
                self.terminate().await;
                break;
            }
        }

        (self.after_stop)(self).await;
    }
    #[instrument(skip(self))]
    async fn terminate(&mut self) {

        // Collect suspend futures for all children
        let suspend_futures: Vec<_> = self.handle.children().iter().map(|item| {
            let child_ref = item.value().clone(); // Clone to take ownership
            async move {
                let _ = child_ref.stop().await;
            }
        }).collect();

        // Wait for all children to suspend concurrently
        join_all(suspend_futures).await;

        trace!(
        actor = self.id.to_string(),
        "All subordinates terminated. Closing mailbox for"
    );

        self.inbox.close();
    }
}
