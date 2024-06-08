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

use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::time::Duration;

use akton_arn::{Arn, ArnBuilder, Category, Company, Domain, Part};
use dashmap::DashMap;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::time::timeout;
use tokio_util::task::TaskTracker;
use tracing::{event, instrument, Level, trace, warn};

use crate::common::{Context, ReactorItem, ReactorMap, StopSignal, SystemSignal};
use crate::message::{Envelope, OutboundEnvelope};
use crate::pool::{PoolBuilder, PoolItem};
use crate::traits::{ActorContext};

use super::{Awake, Idle};

/// Represents an actor in the Akton framework.
///
/// # Type Parameters
/// - `RefType`: The type used for the actor's setup reference.
/// - `State`: The type representing the state of the actor.
pub struct Actor<RefType: Send + 'static, State: Default + Send + Debug + 'static> {
    /// The setup reference for the actor.
    pub setup: RefType,

    /// The context in which the actor operates.
    pub context: Context,

    /// The parent actor's return envelope.
    pub parent: Option<Context>,

    /// The signal used to halt the actor.
    pub halt_signal: StopSignal,

    /// The unique identifier (ARN) for the actor.
    pub key: Arn,

    /// The state of the actor.
    pub state: State,

    /// The task tracker for the actor.
    pub(crate) task_tracker: TaskTracker,

    /// The mailbox for receiving envelopes.
    pub mailbox: Receiver<Envelope>,
    /// The mailbox for receiving envelopes.
    pub(crate) pool_supervisor: DashMap<String, PoolItem>,
}

/// Custom implementation of the `Debug` trait for the `Actor` struct.
///
/// This implementation provides a formatted output for the `Actor` struct, primarily focusing on the `key` field.
impl<RefType: Send + 'static, State: Default + Send + Debug + 'static> Debug
for Actor<RefType, State>
{
    /// Formats the `Actor` struct using the given formatter.
    ///
    /// # Parameters
    /// - `f`: The formatter used for writing formatted output.
    ///
    /// # Returns
    /// A result indicating whether the formatting was successful.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Actor")
            .field("key", &self.key.value) // Only the key field is included in the debug output
            .finish()
    }
}

/// Represents an actor in the awake state.
///
/// # Type Parameters
/// - `State`: The type representing the state of the actor.
impl<State: Default + Send + Debug + 'static> Actor<Awake<State>, State> {
    /// Creates a new outbound envelope for the actor.
    ///
    /// # Returns
    /// An optional `OutboundEnvelope` if the context's outbox is available.
    pub fn new_envelope(&self) -> Option<OutboundEnvelope> {
        if let Some(envelope) = &self.context.outbox {
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

    /// Wakes the actor and processes incoming messages using the provided reactors.
    ///
    /// # Parameters
    /// - `reactors`: A map of reactors to handle different message types.
    #[instrument(skip(reactors, self))]
    pub(crate) async fn wake(&mut self, reactors: ReactorMap<State>) {
        // Call the on_wake setup function
        (self.setup.on_wake)(self);

        let mut yield_counter = 0;
        while let Some(envelope) = self.mailbox.recv().await {
            let type_id = &envelope.message.as_any().type_id().clone();
            tracing::debug!(actor=self.key.value, "Mailbox received {:?} for", &envelope.message);

            // Handle SystemSignal::Terminate to stop the actor

            if let Some(ref pool_id) = &envelope.pool_id {
                // Event: Processing Envelope
                // Description: Processing an envelope for a specific pool.
                // Context: Pool ID and envelope details.
                // trace!(pool_id = ?pool_id, envelope = ?envelope, "Processing envelope for pool.");
                if let Some(mut pool_def) = self.pool_supervisor.get_mut(pool_id) {
                    // First, clone or copy the data needed for the immutable borrow.
                    // NOTE: Cloning the whole pool may be expensive, so consider alternatives if performance is a concern.
                    let pool_clone = pool_def.pool.clone();

                    // Now perform the selection outside the mutable borrowed variable's scope.
                    if let Some(index) = pool_def.strategy.select_context(&pool_clone) {
                        // Access the original data using the index now that we're outside the conflicting borrow.
                        let context = &pool_def.pool[index];
                        trace!(pool_item=context.key.value,index = index, "Emitting to pool item");
                        context.emit_message_async(envelope.message, None).await;
                    }
                }
            } else if let Some(reactor) = reactors.get(type_id) {
                event!(Level::TRACE, "Message reactor found");

                match reactor.value() {
                    ReactorItem::Message(reactor) => {
                        event!(
                            Level::TRACE,
                            "Executing non-future reactor with {} children",
                            &self.context.children().len()
                        );
                        (*reactor)(self, &envelope);
                    }
                    ReactorItem::Future(fut) => {
                        event!(Level::TRACE, "Awaiting future-based reactor");
                        fut(self, &envelope).await;
                    }
                    _ => {
                        tracing::warn!("Unknown ReactorItem type for: {:?}", &type_id.clone());
                    }
                }

                trace!("Message handled by reactor");
            } else if let Some(SystemSignal::Terminate) =
                envelope.message.as_any().downcast_ref::<SystemSignal>()
            {
                tracing::warn!(actor=self.key.value, "Received SystemSignal::Terminate for");

                tracing::trace!(
                    "Terminating {} children",
                    &self.context.children.len()
                );
                for item in &self.context.children {
                    let context = item.value();
                    let _ = context.suspend().await;
                }
                tracing::trace!(
                    "Terminating {} actor managed pools", &self.pool_supervisor.len()
                );
                for pool in &self.pool_supervisor {
                    tracing::trace!(
                    "Terminating pool with id \"{}\".", pool.id
                );
                    for item_context in &pool.pool {
                        trace!(item=item_context.key.value,"Terminating pool item.");
                        let _ = item_context.suspend().await;
                    }
                }

                trace!(actor=self.key.value,"All subordinates terminated. Closing mailbox for");
                self.mailbox.close();
            }

            // Yield less frequently to reduce context switching
            yield_counter += 1;
            if yield_counter % 100 == 0 {
                tokio::task::yield_now().await;
            }
        }
        // Call the on_before_stop setup function
        (self.setup.on_before_stop)(self);
        tracing::trace!(actor=self.key.value,"called on_before_stop for");
        if let Some(ref on_before_stop_async) = self.setup.on_before_stop_async {
            // Add a timeout to prevent hanging indefinitely
            match timeout(Duration::from_secs(5), on_before_stop_async(self)).await {
                Ok(_) => {
                    tracing::info!(actor=self.key.value,"called on_before_stop_async for");
                }
                Err(e) => {
                    tracing::error!("on_before_stop_async timed out or failed: {:?}", e);
                }
            }
        }

        // Call the on_stop setup function
        (self.setup.on_stop)(self);
        tracing::trace!(actor=self.key.value,"called on_stop for");
    }
}

/// Represents an actor in the idle state.
///
/// # Type Parameters
/// - `State`: The type representing the state of the actor.
impl<State: Default + Send + Debug + 'static> Actor<Idle<State>, State> {
    /// Creates a new actor with the given ID, state, and optional parent context.
    ///
    /// # Parameters
    /// - `id`: The identifier for the new actor.
    /// - `state`: The initial state of the actor.
    /// - `parent_context`: An optional parent context for the new actor.
    ///
    /// # Returns
    /// A new `Actor` instance.
    #[instrument(skip(state, parent_context))]
    pub(crate) fn new(id: &str, state: State, parent_context: Option<Context>) -> Self {
        // Create a channel with a buffer size of 255 for the actor's mailbox
        let (outbox, mailbox) = channel(255);

        // Initialize context and task tracker based on whether a parent context is provided
        let (parent, key, task_tracker, context) =
            if let Some(parent_context) = parent_context {
                let mut key = parent_context.key.clone();
                key.append_part(id);
                trace!("NEW ACTOR: {}", &key.value);

                let context = Context {
                    key: key.clone(),
                    outbox: Some(outbox.clone()),
                    parent: Some(Box::new(parent_context.clone())),
                    task_tracker: TaskTracker::new(),
                    ..Default::default()
                };

                (
                    Some(parent_context.clone()),
                    key,
                    parent_context.task_tracker.clone(),
                    context,
                )
            } else {
                // If no parent context is provided, initialize a new context
                let key = ArnBuilder::new()
                    .add::<Domain>("akton")
                    .add::<Category>("system")
                    .add::<Company>("framework")
                    .add::<Part>(id)
                    .build();
                trace!("NEW ACTOR: {}", &key.value);

                let context = Context {
                    key,
                    outbox: Some(outbox.clone()),
                    parent: None,
                    task_tracker: TaskTracker::new(),
                    ..Default::default()
                };
                let key = context.key.clone();

                (
                    None,
                    key,
                    TaskTracker::new(),
                    context,
                )
            };

        // Ensure the mailbox and outbox are not closed
        debug_assert!(!mailbox.is_closed(), "Actor mailbox is closed in new");
        debug_assert!(!outbox.is_closed(), "Outbox is closed in new");


        // Create and return the new actor instance
        Actor {
            setup: Idle::default(),
            context,
            parent,
            halt_signal: Default::default(),
            key,
            state,
            task_tracker,
            mailbox,
            pool_supervisor: Default::default(),
        }
    }

    /// Activates the actor, optionally with a pool builder.
    ///
    /// # Parameters
    /// - `builder`: An optional `PoolBuilder` to initialize a `PoolSupervisor` to manage pool items .
    ///
    /// # Returns
    /// The actor's context after activation.
    #[instrument(skip(self), fields(key = self.key.value))]
    pub fn activate(
        self,
        builder: Option<PoolBuilder>,
    ) -> Pin<Box<dyn Future<Output=anyhow::Result<Context>> + Send + 'static>> {
        Box::pin(async move {
            // Store and activate all supervised children if a builder is provided
            let mut actor = self;
            let reactors = mem::take(&mut actor.setup.reactors);
            let context = actor.context.clone();


            // If a pool builder is provided, spawn the supervisor
            if let Some(builder) = builder {
                trace!(id = actor.key.value, "PoolBuilder provided.");
                let moved_context = actor.context.clone();
                actor.pool_supervisor = builder.spawn(&moved_context).await?;
            }

            // here we transition from an Actor<Idle> to an Actor<Awake>
            let active_actor: Actor<Awake<State>, State> = actor.into();

            // makes actor live for static, required for the `wake` function
            let actor = Box::leak(Box::new(active_actor));
            debug_assert!(
                !actor.mailbox.is_closed(),
                "Actor mailbox is closed in spawn"
            );

            // TODO: we need to store this join handle
            // Spawn the actor's wake task
            let _ = &context.task_tracker.spawn(actor.wake(reactors));

            context.task_tracker.close();

            Ok(context.clone())
        })
    }
}

/// Represents an actor in the awake state.
///
/// # Type Parameters
/// - `State`: The type representing the state of the actor.
impl<State: Default + Send + Debug + 'static> Actor<Awake<State>, State> {
    /// Terminates the actor by setting the halt signal.
    ///
    /// This method sets the halt signal to true, indicating that the actor should stop processing.
    #[instrument(skip(self))]
    pub(crate) fn terminate(&self) {
        // Load the current value of the halt signal with sequential consistency ordering
        self.halt_signal.load(Ordering::SeqCst);

        // Store `true` in the halt signal to indicate termination
        self.halt_signal.store(true, Ordering::SeqCst);
    }
}
