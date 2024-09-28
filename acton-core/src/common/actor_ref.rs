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

use acton_ern::{Ern, UnixTime};
use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio_util::task::TaskTracker;
use tracing::{info, instrument, trace, warn, debug};

use crate::actor::{Idle, ManagedActor};
use crate::common::{BrokerRef, OutboundEnvelope, Outbox, ParentRef};
use crate::message::{ReturnAddress, SystemSignal};
use crate::traits::{Actor, Subscriber};

/// Represents the context in which an actor operates.
#[derive(Debug, Clone)]
pub struct ActorRef {
    /// The unique identifier (ARN) for the context.
    ern: Ern<UnixTime>,
    /// The outbound channel for sending messages.
    pub(crate) outbox: Outbox,
    /// The task tracker for the actor.
    tracker: TaskTracker,
    /// The actor's optional parent context.
    pub parent: Option<Box<ParentRef>>,
    /// The system broker for the actor.
    pub broker: Box<Option<BrokerRef>>,
    children: DashMap<String, ActorRef>,
}

impl Default for ActorRef {
    fn default() -> Self {
        let (outbox, _) = mpsc::channel(1);
        ActorRef {
            ern: Ern::default(),
            outbox,
            tracker: TaskTracker::new(),
            parent: None,
            broker: Box::new(None),
            children: DashMap::new(),
        }
    }
}

impl Subscriber for ActorRef {
    fn get_broker(&self) -> Option<BrokerRef> {
        *self.broker.clone()
    }
}

impl PartialEq for ActorRef {
    fn eq(&self, other: &Self) -> bool {
        self.ern == other.ern
    }
}

impl Eq for ActorRef {}

impl Hash for ActorRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.ern.hash(state);
    }
}

impl ActorRef {
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
    /// - `child`: A [`ManagedActor`] instance representing the child actor to be supervised.
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
        child: ManagedActor<Idle, State>,
    ) -> anyhow::Result<()> {
        debug!("Adding child actor with id: {}", child.ern);
        let context = child.activate().await;
        let id = context.ern.clone();
        debug!("Now have child id in context: {}", id);
        self.children.insert(id.to_string(), context);

        Ok(())
    }
}

#[async_trait]
impl Actor for ActorRef {
    /// Returns the return address for the actor.
    #[instrument(skip(self))]
    fn return_address(&self) -> OutboundEnvelope {
        let outbox = self.outbox.clone();
        let return_address = ReturnAddress::new(outbox, self.ern.clone());
        OutboundEnvelope::new(return_address)
    }
    // #[instrument(Level::TRACE, skip(self), fields(child_count = self.children.len()))]
    fn children(&self) -> DashMap<String, ActorRef> {
        self.children.clone()
    }

    #[instrument(skip(self))]
    fn find_child(&self, arn: &Ern<UnixTime>) -> Option<ActorRef> {
        debug!("Searching for child with ARN: {}", arn);
        self.children.get(&arn.to_string()).map(|item|
        item.value().clone()
        )
    }

    /// Returns the task tracker for the actor.
    fn tracker(&self) -> TaskTracker {
        self.tracker.clone()
    }

    fn set_ern(&mut self, ern: Ern<UnixTime>) {
        self.ern = ern;
    }

    fn ern(&self) -> Ern<UnixTime> {
        self.ern.clone()
    }

    fn clone_ref(&self) -> ActorRef {
        self.clone()
    }

    #[allow(clippy::manual_async_fn)]
    /// Suspends the actor.
    fn suspend(&self) -> impl Future<Output=anyhow::Result<()>> + Send + Sync + '_ {
        async move {
            let tracker = self.tracker().clone();

            let actor = self.return_address().clone();

            // Event: Sending Terminate Signal
            // Description: Sending a terminate signal to the actor.
            // Context: Target actor key.
            warn!(actor = self.ern.to_string(), "Sending Terminate to");
            actor.reply(SystemSignal::Terminate)?;

            // Event: Waiting for Actor Tasks
            // Description: Waiting for all actor tasks to complete.
            // Context: None
            trace!("Waiting for all actor tasks to complete.");
            tracker.wait().await;

            // Event: Actor Terminated
            // Description: The actor and its subordinates have been terminated.
            // Context: None
            info!(
                actor = self.ern.to_string(),
                "The actor and its subordinates have been terminated."
            );
            Ok(())
        }
    }
}
