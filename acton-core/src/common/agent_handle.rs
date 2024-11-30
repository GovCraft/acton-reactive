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
use std::fmt::Debug;
use std::future::Future;
use std::hash::{Hash, Hasher};

use acton_ern::Ern;
use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio_util::task::TaskTracker;
use tracing::{error, instrument, trace, warn};

use crate::actor::{Idle, ManagedAgent};
use crate::common::{BrokerRef, OutboundEnvelope, Outbox, ParentRef};
use crate::message::{BrokerRequest, MessageAddress, SystemSignal};
use crate::prelude::ActonMessage;
use crate::traits::{Actor, Broker, Subscriber};

/// Represents the context in which an actor operates.
#[derive(Debug, Clone)]
pub struct AgentHandle {
    /// The unique identifier (ARN) for the context.
    pub(crate) id: Ern,
    /// The outbound channel for sending messages.
    pub(crate) outbox: Outbox,
    /// The task tracker for the actor.
    tracker: TaskTracker,
    /// The actor's optional parent context.
    pub parent: Option<Box<ParentRef>>,
    /// The system broker for the actor.
    pub broker: Box<Option<BrokerRef>>,
    children: DashMap<String, AgentHandle>,
}

impl Default for AgentHandle {
    fn default() -> Self {
        let (outbox, _) = mpsc::channel(1);
        AgentHandle {
            id: Ern::default(),
            outbox,
            tracker: TaskTracker::new(),
            parent: None,
            broker: Box::new(None),
            children: DashMap::new(),
        }
    }
}

impl Subscriber for AgentHandle {
    fn get_broker(&self) -> Option<BrokerRef> {
        *self.broker.clone()
    }
}

impl PartialEq for AgentHandle {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for AgentHandle {}

impl Hash for AgentHandle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl AgentHandle {
    /// Supervises a child actor by activating it and tracking its context.
    ///
    /// This asynchronous method adds a child actor to the supervision hierarchy managed by this
    /// `ActorRef`. It performs the following steps:
    ///
    /// 1. Logs the addition of the child actor with its unique identifier (`ern`).
    /// 2. Activates the child actor by calling its `activate` method asynchronously.
    /// 3. Retrieves the `ern` (unique identifier) from the child’s context.
    /// 4. Inserts the child's context into the `children` map of the supervising actor,
    ///    using the `ern` as the key.
    ///
    /// # Type Parameters
    ///
    /// - `State`: Represents the state type associated with the child actor. It must implement
    ///   the [`Default`], [`Send`], and [`Debug`] traits.
    ///
    /// # Parameters
    ///
    /// - `child`: A [`ManagedAgent`] instance representing the child actor to be supervised.
    ///
    /// # Returns
    ///
    /// A [`Result`] which is:
    /// - `Ok(())` if the child actor was successfully supervised and added to the supervision
    ///   hierarchy.
    /// - An error of type [`anyhow::Error`] if any step of the supervision process fails.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - The child actor fails to activate.
    /// - Inserting the child context into the `children` map fails.
    #[instrument(skip(self))]
    pub async fn supervise<State: Default + Send + Debug>(
        &self,
        child: ManagedAgent<Idle, State>,
    ) -> anyhow::Result<AgentHandle> {
        trace!("Adding child actor with id: {}", child.id);
        let handle = child.start().await;
        let id = handle.id.clone();
        trace!("Now have child id in context: {}", id);
        self.children.insert(id.to_string(), handle.clone());

        Ok(handle)
    }
}

impl Broker for AgentHandle {
    #[instrument(skip(self), name = "broadcast")]
    fn broadcast(&self, message: impl ActonMessage) -> impl Future<Output = ()> + Send + Sync + '_ {
        trace!("Looking for a broker to broadcast message.");
        async move {
            if let Some(broker) = self.broker.as_ref() {
                broker.send(BrokerRequest::new(message)).await;
            } else {
                error!("No broker found to broadcast message.");
            }
        }
    }
}

#[async_trait]
impl Actor for AgentHandle {
    /// Returns the message address for this agent.
    fn reply_address(&self) -> MessageAddress {
        MessageAddress::new(self.outbox.clone(), self.id.clone())
    }

    /// Returns an envelope for the specified recipient and message, ready to send.
    #[instrument(skip(self))]
    fn create_envelope(&self, recipient_address: Option<MessageAddress>) -> OutboundEnvelope {
        trace!("self id is {}", self.id);
        let return_address = self.reply_address();
        trace!("return_address is {}", return_address.sender.root);
        if let Some(recipient) = recipient_address {
            OutboundEnvelope::new_with_recipient(return_address, recipient)
        } else {
            OutboundEnvelope::new(return_address)
        }
    }

    fn children(&self) -> DashMap<String, AgentHandle> {
        self.children.clone()
    }

    #[instrument(skip(self))]
    fn find_child(&self, arn: &Ern) -> Option<AgentHandle> {
        trace!("Searching for child with ARN: {}", arn);
        self.children
            .get(&arn.to_string())
            .map(|item| item.value().clone())
    }

    /// Returns the task tracker for the actor.
    fn tracker(&self) -> TaskTracker {
        self.tracker.clone()
    }
    fn id(&self) -> Ern {
        self.id.clone()
    }

    fn name(&self) -> String {
        self.id.root.to_string()
    }

    fn clone_ref(&self) -> AgentHandle {
        self.clone()
    }

    #[allow(clippy::manual_async_fn)]
    #[instrument(skip(self))]
    /// Suspends the actor.
    fn stop(&self) -> impl Future<Output = anyhow::Result<()>> + Send + Sync + '_ {
        async move {
            let tracker = self.tracker();

            let actor = self.create_envelope(None).clone();

            // Event: Sending Terminate Signal
            // Description: Sending a terminate signal to the actor.
            // Context: Target actor key.
            trace!(actor = self.id.to_string(), "Sending Terminate to");
            actor.reply(SystemSignal::Terminate)?;

            // Event: Waiting for Actor Tasks
            // Description: Waiting for all actor tasks to complete.
            // Context: None
            trace!("Waiting for all actor tasks to complete.");
            tracker.wait().await;

            // Event: Actor Terminated
            // Description: The actor and its subordinates have been terminated.
            // Context: None
            trace!(
                actor = self.id.to_string(),
                "The actor and its subordinates have been terminated."
            );
            Ok(())
        }
    }
}
