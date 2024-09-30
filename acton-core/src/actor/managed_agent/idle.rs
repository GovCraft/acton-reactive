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
use std::fmt::Debug;
use std::future::Future;
use std::mem;

use acton_ern::{Ern, UnixTime};
use dashmap::DashMap;
use tokio::sync::mpsc::channel;
use tracing::*;

use crate::actor::{ActorConfig, ManagedAgent, Started};
use crate::common::{ActonInner, AgentHandle, AgentRuntime, AsyncLifecycleHandler, Envelope, FutureBox, MessageHandler, OutboundEnvelope, ReactorItem};
use crate::message::MessageContext;
use crate::prelude::ActonMessage;
use crate::traits::Actor;

pub struct Idle;

impl<State: Default + Send + Debug + 'static> ManagedAgent<Idle, State> {
    /// Adds an asynchronous message handler for a specific message type.
    ///
    /// # Parameters
    /// - `message_processor`: The function to handle the message.
    #[instrument(skip(self, message_processor), level = "debug")]
    pub fn act_on<M>(
        &mut self,
        message_processor: impl for<'a> Fn(
            &'a mut ManagedAgent<Started, State>,
            &'a mut MessageContext<M>,
        ) -> FutureBox
        + Send
        + Sync
        + 'static,
    ) -> &mut Self
    where
        M: ActonMessage + Clone + Send + Sync + 'static,
    {
        let type_id = TypeId::of::<M>();
        trace!(type_name=std::any::type_name::<M>(),type_id=?type_id, " Adding message handler");
        // Create a boxed handler for the message type.
        let handler_box = Box::new(
            move |actor: &mut ManagedAgent<Started, State>,
                  envelope: &mut Envelope|
                  -> FutureBox {
                trace!("Creating handler for message type: {:?}", std::any::type_name::<M>());

                let envelope_type_id = envelope.message.as_any().type_id();
                trace!(
                "Attempting to downcast message: expected_type_id = {:?}, envelope_type_id = {:?}",
                type_id, envelope_type_id
            );
                if let Some(concrete_msg) = downcast_message::<M>(&*envelope.message) {
                    trace!(
                        "Downcast message to name {} and concrete type: {:?}",
                        std::any::type_name::<M>(),
                        type_id
                    );

                    let message = concrete_msg.clone();
                    let sent_time = envelope.timestamp;
                    let mut event_record = {
                        let msg_name = std::any::type_name::<M>();
                        let sender = envelope.reply_to.sender.root.to_string();
                        let recipient = envelope.recipient.sender.root.to_string();
                        let origin_envelope = OutboundEnvelope::new_with_recipient(envelope.reply_to.clone(), envelope.recipient.clone());
                        let reply_envelope = OutboundEnvelope::new_with_recipient(envelope.recipient.clone(), envelope.reply_to.clone());
                        trace!("sender {sender}::{msg_name}",);
                        trace!("recipient {recipient}::{msg_name}",);
                        MessageContext {
                            message,
                            timestamp: sent_time,
                            origin_envelope,
                            reply_envelope,
                        }
                    };

                    // Call the user-provided function and get the future.
                    let user_future = message_processor(actor, &mut event_record);

                    // Automatically box and pin the user future.
                    Box::pin(user_future)
                } else {
                    error!(
                        type_name = std::any::type_name::<M>(),
                        "Should never get here, message failed to downcast"
                    );
                    // Return an immediately resolving future if downcast fails.
                    Box::pin(async {})
                }
            },
        );

        // Insert the handler into the reactors map.
        self.reactors.insert(type_id, ReactorItem::FutureReactor(handler_box));
        self
    }


    /// Sets the reactor to be called when the actor wakes up.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn after_start<F, Fut>(&mut self, f: F) -> &mut Self
    where
        F: for<'b> Fn(&'b ManagedAgent<Started, State>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output=()> + Send + Sync + 'static,
    {
        // Create a boxed handler that can be stored in the HashMap.
        self.after_start = Box::new(move |agent| Box::pin(f(agent)) as FutureBox);
        self
    }
    /// Sets the reactor to be called when the actor wakes up.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn before_start<F, Fut>(&mut self, f: F) -> &mut Self
    where
        F: for<'b> Fn(&'b ManagedAgent<Started, State>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output=()> + Send + Sync + 'static,
    {
        // Create a boxed handler that can be stored in the HashMap.
        self.before_start = Box::new(move |agent| Box::pin(f(agent)) as FutureBox);
        self
    }

    /// Sets the reactor to be called when the actor stops processing messages in its mailbox.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn after_stop<F, Fut>(&mut self, f: F) -> &mut Self
    where
        F: for<'b> Fn(&'b ManagedAgent<Started, State>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output=()> + Send + Sync + 'static,
    {
        self.after_stop = Box::new(move |agent| Box::pin(f(agent)) as FutureBox);
        self
    }
    /// Sets the reactor to be called just before the actor stops processing messages in its mailbox.
    ///
    /// # Parameters
    /// - `life_cycle_event_reactor`: The function to be called.
    pub fn before_stop<F, Fut>(&mut self, f: F) -> &mut Self
    where
        F: for<'b> Fn(&'b ManagedAgent<Started, State>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output=()> + Send + Sync + 'static,
    {
        self.before_stop = Box::new(move |agent| Box::pin(f(agent)) as FutureBox);
        self
    }

    /// Creates and supervises a new actor with the given ID and state.
    ///
    /// # Parameters
    /// - `id`: The identifier for the new actor.
    ///
    /// # Returns
    /// A new `Actor` instance in the idle state.
    #[instrument(skip(self))]
    pub async fn create_child(&self) -> ManagedAgent<Idle, State> {
        ManagedAgent::new(&Some(self.runtime.clone()), None).await
    }

    #[instrument]
    pub(crate) async fn new(acton: &Option<AgentRuntime>, config: Option<ActorConfig>) -> Self {
        let mut managed_actor: ManagedAgent<Idle, State> = ManagedAgent::default();

        if let Some(config) = &config {
            managed_actor.handle.id = config.ern();
            managed_actor.parent = config.parent().clone();
            managed_actor.handle.broker = Box::new(config.get_broker().clone());
            if let Some(broker) = config.get_broker().clone() {
                managed_actor.broker = broker;
            }
        }

        debug_assert!(
            !managed_actor.inbox.is_closed(),
            "Actor mailbox is closed in new"
        );

        trace!("NEW ACTOR: {}", &managed_actor.handle.id());

        managed_actor.runtime = acton.clone().unwrap_or_else(|| AgentRuntime(ActonInner {
            broker: managed_actor.handle.broker.clone().unwrap_or_default(),
            ..Default::default()
        }));

        managed_actor.id = managed_actor.handle.id();

        managed_actor
    }

    #[instrument(skip(self))]
    pub async fn start(mut self) -> AgentHandle {
        let is_broker = self.id.root.to_string().starts_with("broker");
        trace!("The model is {:?}", self.model);

        let reactors = mem::take(&mut self.reactors);
        let actor_ref = self.handle.clone();
        debug!("actor_ref before spawn: {:?}", actor_ref.id.root.to_string());
        let active_actor: ManagedAgent<Started, State> = self.into();
        let actor = Box::leak(Box::new(active_actor));

        debug_assert!(
            !actor.inbox.is_closed(),
            "Actor mailbox is closed in activate"
        );
        (actor.before_start)(actor).await;
        actor_ref.tracker().spawn(actor.wake(reactors));
        actor_ref.tracker().close();
        debug!("actor_ref after spawn: {:?}", actor_ref.id.root.to_string());

        actor_ref
    }
}

impl<State: Default + Send + Debug + 'static> From<ManagedAgent<Idle, State>>
for ManagedAgent<Started, State>
{
    fn from(value: ManagedAgent<Idle, State>) -> Self {
        let is_broker = value.id.root.to_string().starts_with("broker");

        let on_starting = value.before_start;
        let on_start = value.after_start;
        let on_stopped = value.after_stop;
        let on_before_stop = value.before_stop;
        let halt_signal = value.halt_signal;
        let parent = value.parent;
        let id = value.id;
        let tracker = value.tracker;
        let acton = value.runtime;
        let reactors = value.reactors;


        debug_assert!(
            !value.inbox.is_closed(),
            "Actor mailbox is closed before conversion in From<Actor<Idle, State>>"
        );

        let inbox = value.inbox;
        let handle = value.handle;
        let model = value.model;
        let broker = value.broker;

        // tracing::trace!("Mailbox is not closed, proceeding with conversion");
        if handle.children().is_empty() {
            trace!(
                "child count before Actor creation {}",
                handle.children().len()
            );
        }
        // Create and return the new actor in the running state
        ManagedAgent::<Started, State> {
            handle,
            parent,
            halt_signal,
            id,
            runtime: acton,
            model,
            tracker,
            inbox,
            before_start: on_starting,
            after_start: on_start,
            before_stop: on_before_stop,
            after_stop: on_stopped,
            broker,
            reactors,
            _actor_state: Default::default(),
        }
    }
}

impl<State: Default + Send + Debug + 'static> Default
for ManagedAgent<Idle, State>
{
    fn default() -> Self {
        let (outbox, inbox) = channel(255);
        let id: Ern<UnixTime> = Default::default();
        let mut handle: AgentHandle = Default::default();
        handle.id = id.clone();
        handle.outbox = outbox.clone();

        ManagedAgent::<Idle, State> {
            handle,
            id,
            inbox,
            before_start: Box::new(|a: &'_ ManagedAgent<Started, State>| default_handler(a)),
            after_start: Box::new(|a: &'_ ManagedAgent<Started, State>| default_handler(a)),
            before_stop: Box::new(|a: &'_ ManagedAgent<Started, State>| default_handler(a)),
            after_stop: Box::new(|a: &'_ ManagedAgent<Started, State>| default_handler(a)),
            model: State::default(),
            broker: Default::default(),
            parent: Default::default(),
            runtime: Default::default(),
            halt_signal: Default::default(),
            tracker: Default::default(),
            reactors: Default::default(),
            _actor_state: Default::default(),
        }
    }
}

fn default_handler<State: Debug + Send + Default>(
    _actor: &'_ ManagedAgent<Started, State>,
) -> FutureBox {
    Box::pin(async {})
}

// Function to downcast the message to the original type.
pub fn downcast_message<T: 'static>(msg: &dyn ActonMessage) -> Option<&T> {
    msg.as_any().downcast_ref::<T>()
}
