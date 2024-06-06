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

use crate::common::{
    Awake, Context, Idle, OutboundEnvelope, ReactorItem, ReactorMap, StopSignal, SystemSignal,
};
use crate::traits::{ActorContext, SupervisorContext};
use akton_arn::{Arn, ArnBuilder, Category, Company, Domain, Part};
use std::fmt::Debug;
use std::mem;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc::{channel, Receiver};
use tokio_util::task::TaskTracker;
use tracing::{error, event, instrument, Level, trace};
use tracing::field::Empty;

use super::signal::SupervisorSignal;
use super::Envelope;
use super::PoolBuilder;
use std::fmt;
use std::fmt::Formatter;
use std::future::Future;
use std::pin::Pin;
use futures::SinkExt;

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
    pub(crate) parent_return_envelope: OutboundEnvelope,

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
    pub fn new_parent_envelope(&self) -> OutboundEnvelope {
        self.parent_return_envelope.clone()
    }

    /// Wakes the actor and processes incoming messages using the provided reactors.
    ///
    /// # Parameters
    /// - `reactors`: A map of reactors to handle different message types.
    #[instrument(skip(reactors, self), fields(key = self.key.value))]
    pub(crate) async fn wake(&mut self, reactors: ReactorMap<State>) {
        // Call the on_wake setup function
        (self.setup.on_wake)(self);

        let mut yield_counter = 0;
        while let Some(mut envelope) = self.mailbox.recv().await {
            let type_id = &envelope.message.as_any().type_id().clone();
            event!(Level::TRACE, "Mailbox received {:?}", &envelope.message);

            if let Some(reactor) = reactors.get(&type_id) {
                event!(Level::TRACE, "Message reactor found");

                match reactor.value() {
                    ReactorItem::Message(reactor) => {
                        event!(Level::TRACE, "Executing non-future reactor with {} children", &self.context.children().len());
                        (*reactor)(self, &envelope);
                    }
                    ReactorItem::Future(fut) => {
                        event!(Level::TRACE, "Awaiting future-based reactor");
                        fut(self, &envelope).await;
                    }
                    _ => {
                        tracing::warn!("Unknown ReactorItem type for: {:?}", &envelope.message);
                    }
                }

                trace!("Message handled by reactor");
            } else {
                tracing::warn!("No reactor found for: {:?} ", &envelope.message);
            }

            // Handle SystemSignal::Terminate to stop the actor
            trace!("Checking for SystemSignal::Terminate");
            if let Some(SystemSignal::Terminate) = envelope.message.as_any().downcast_ref::<SystemSignal>() {
                event!(Level::TRACE, "Received SystemSignal::Terminate");

                event!(Level::TRACE, "Terminating {} children", &self.context.children.len());
                for item in &self.context.children {
                    let context = item.value();
                    let tracker = item.get_task_tracker().clone();
                    let _ = context.terminate().await;
                    tracker.wait().await;
                }

                // Call the on_before_stop setup function
                (self.setup.on_before_stop)(self);
                if let Some(ref on_before_stop_async) = self.setup.on_before_stop_async {
                    on_before_stop_async(self).await;
                }
                break;
            }

            // Yield less frequently to reduce context switching
            yield_counter += 1;
            if yield_counter % 100 == 0 {
                tokio::task::yield_now().await;
            }
        }

        // Call the on_stop setup function
        (self.setup.on_stop)(self);
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
        let (parent_return_envelope, key, task_tracker, context) = if let Some(parent_context) = parent_context {
            let mut key = parent_context.key().clone();
            key.append_part(id);
            trace!("NEW ACTOR: {}", &key.value);

            let context = Context {
                key: key.clone(),
                outbox: Some(outbox.clone()),
                supervisor_task_tracker: TaskTracker::new(),
                supervisor_outbox: parent_context.return_address().reply_to,
                task_tracker: TaskTracker::new(),
                ..Default::default()
            };

            // Ensure the parent context's outbox and supervisor outbox are not closed
            debug_assert!(
                parent_context
                    .outbox
                    .as_ref()
                    .map_or(true, |outbox| !outbox.is_closed()),
                "Parent context outbox is closed in new"
            );
            debug_assert!(
                parent_context
                    .supervisor_outbox
                    .as_ref()
                    .map_or(true, |outbox| !outbox.is_closed()),
                "Parent context supervisor outbox is closed in new"
            );

            (
                parent_context.return_address().clone(),
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
                supervisor_task_tracker: TaskTracker::new(),
                supervisor_outbox: None,
                task_tracker: TaskTracker::new(),
                ..Default::default()
            };
            let key = context.key().clone();

            (
                context.return_address().clone(),
                key,
                TaskTracker::new(),
                context,
            )
        };

        // Ensure the mailbox and outbox are not closed
        debug_assert!(!mailbox.is_closed(), "Actor mailbox is closed in new");
        debug_assert!(!outbox.is_closed(), "Outbox is closed in new");

        // Ensure the supervisor outbox is not closed if it exists
        if let Some(ref supervisor_outbox) = context.supervisor_outbox {
            debug_assert!(
                !supervisor_outbox.is_closed(),
                "Supervisor outbox is closed in new"
            );
        }

        // Create and return the new actor instance
        let actor = Actor {
            setup: Idle::default(),
            context,
            parent_return_envelope,
            halt_signal: Default::default(),
            key,
            state,
            task_tracker,
            mailbox,
        };
        actor
    }

    /// Activates the actor, optionally with a pool builder.
    ///
    /// # Parameters
    /// - `builder`: An optional `PoolBuilder` to initialize supervised children.
    ///
    /// # Returns
    /// The actor's context after activation.
    #[instrument(skip(self), fields(key = self.key.value))]
    pub fn activate(self, builder: Option<PoolBuilder>) -> Pin<Box<dyn Future<Output=anyhow::Result<Context>> + Send + 'static>> {
        Box::pin(async move {
            // Store and activate all supervised children if a builder is provided
            let mut actor = self;
            let reactors = mem::take(&mut actor.setup.reactors);
            let mut context = actor.context.clone();
            event!(Level::TRACE, idle_child_count=context.children().len());
            //activate all children first
            if let Some(builder) = builder {
                event!(Level::TRACE, "PoolBuilder provided.");
                // If a pool builder is provided, spawn the supervisor
                let actor_context = actor.context.clone();
                let moved_context = actor.context.clone();
                let mut supervisor = builder.spawn(&moved_context).await?;
                context.supervisor_outbox = Some(supervisor.outbox.clone());
                context.supervisor_task_tracker = supervisor.task_tracker.clone();
                let active_actor: Actor<Awake<State>, State> = actor.into();
                if active_actor.context.children().len() > 0 {
                    tracing::trace!("Child pool count after awake actor creation {}", active_actor.context.children().len());
                }

                let mut actor = active_actor;

                let actor_tracker = &context.task_tracker.clone();
                debug_assert!(
                    !actor.mailbox.is_closed(),
                    "Actor mailbox is closed in spawn_with_pools"
                );

                // Spawn the actor's wake task
                actor_tracker.spawn(async move { actor.wake(reactors).await });
                debug_assert!(
                    !supervisor.mailbox.is_closed(),
                    "Supervisor mailbox is closed in spawn_with_pools"
                );

                let supervisor_tracker = supervisor.task_tracker.clone();

                // Spawn the supervisor's wake task
                supervisor_tracker.spawn(async move { supervisor.wake_supervisor().await });
                // Close the supervisor task tracker
                supervisor_tracker.close();
            } else {
                event!(Level::TRACE, "Activating with no PoolBuilder.");
                // If no builder is provided, activate the actor directly
                let active_actor: Actor<Awake<State>, State> = actor.into();
                tracing::trace!("Child count after awake actor creation {}", active_actor.context.children().len());
                let mut actor = active_actor;

                let actor_tracker = &context.task_tracker.clone();
                debug_assert!(
                    !actor.mailbox.is_closed(),
                    "Actor mailbox is closed in spawn"
                );

                // Spawn the actor's wake task
                actor_tracker.spawn(async move { actor.wake(reactors).await });
                // Close the supervisor task tracker
                let _ = &context.supervisor_task_tracker().close();
            }
            // Close the actor's task tracker
            context.task_tracker.close();
            if context.children().len() > 0 {
                tracing::trace!("Child count before returning context {}", context.children().len());
            }

            Ok(context.clone())
        })
    }
}

/// Represents an actor in the awake state.
///
/// # Type Parameters
/// - `State`: The type representing the state of the actor.
impl<State: Default + Clone + Send + Debug + 'static> Actor<Awake<State>, State> {
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