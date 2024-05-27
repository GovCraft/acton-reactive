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
use akton_arn::Arn;
use std::fmt::Debug;
use std::mem;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc::{channel, Receiver};
use tokio_util::task::TaskTracker;
use tracing::instrument;

use super::signal::SupervisorSignal;
use super::Envelope;
use super::PoolBuilder;
use std::fmt;
use std::fmt::Formatter;

pub struct Actor<RefType: Send + 'static, State: Default + Clone + Send + Debug + 'static> {
    pub ctx: RefType,
    pub context: Context,
    pub(crate) parent_return_envelope: OutboundEnvelope,
    pub halt_signal: StopSignal,
    pub key: Arn,
    pub state: State,
    pub(crate) task_tracker: TaskTracker,
    pub mailbox: Receiver<Envelope>,
}
impl<RefType: Send + 'static, State: Default + Clone + Send + Debug + 'static> Debug
    for Actor<RefType, State>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Actor")
            //            .field("mailbox", &self.mailbox)
            //           .field("parent_return_envelope", &self.parent_return_envelope)
            //.field("context", &self.context)
            .field("task_tracker", &self.task_tracker)
            .finish()
    }
}

impl<State: Default + Clone + Send + Debug + 'static> Actor<Awake<State>, State> {
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
    pub fn new_parent_envelope(&self) -> OutboundEnvelope {
        self.parent_return_envelope.clone()
    }
    pub(crate) async fn wake(&mut self, reactors: ReactorMap<State>)
    //  where
    //      State: Send + 'static,
    {
        (self.ctx.on_wake)(self);

        let mut yield_counter = 0;
        while let Some(mut envelope) = self.mailbox.recv().await {
            let type_id = envelope.message.as_any().type_id();

            if let Some(reactor) = reactors.get(&type_id) {
                match reactor.value() {
                    ReactorItem::Message(reactor) => {
                        (*reactor)(self, &envelope);
                    }
                    ReactorItem::Future(fut) => {
                        fut(self, &envelope).await;
                    }
                    _ => {}
                }
            }

            // Match on a mutable reference to the message
            if let Some(SupervisorSignal::Inspect(response_channel)) = envelope
                .message
                .as_any_mut()
                .downcast_mut::<SupervisorSignal<State>>()
            {
                let actor_state = self.state.clone();
                // Take the Option out of the response channel
                if let Some(response_channel) = response_channel.take() {
                    let _ = response_channel.send(actor_state);
                }
            }
            if let Some(SystemSignal::Terminate) =
                envelope.message.as_any().downcast_ref::<SystemSignal>()
            {
                (self.ctx.on_before_stop)(self);
                if let Some(ref on_before_stop_async) = self.ctx.on_before_stop_async {
                    (on_before_stop_async)(self).await;
                }
                break;
            }

            // Yield less frequently to reduce context switching
            yield_counter += 1;
            if yield_counter % 100 == 0 {
                tokio::task::yield_now().await;
            }
        }
        (self.ctx.on_stop)(self);
    }
}

impl<State: Default + Clone + Send + Debug + 'static> Actor<Idle<State>, State> {
    #[instrument(skip(state, id, parent_context))]
    pub(crate) fn new(id: &str, state: State, parent_context: Option<Context>) -> Self {
        let (outbox, mailbox) = channel(255);
        let (parent_return_envelope, key, task_tracker, context) =
            if let Some(parent_context) = parent_context {
                let mut key = parent_context.key().clone();
                key.append_part(id);

                //                let context = parent_context.clone();
                let context = Context {
                    key: key.clone(),
                    outbox: Some(outbox.clone()),
                    supervisor_task_tracker: TaskTracker::new(),
                    supervisor_outbox: parent_context.return_address().reply_to,
                    task_tracker: TaskTracker::new(),
                };

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
                let key = Arn::default();
                let context = Context {
                    key: key.clone(),
                    outbox: Some(outbox.clone()),
                    supervisor_task_tracker: TaskTracker::new(),
                    supervisor_outbox: None,
                    task_tracker: TaskTracker::new(),
                };

                (
                    context.return_address().clone(),
                    key,
                    TaskTracker::new(),
                    context,
                )
            };

        debug_assert!(!mailbox.is_closed(), "Actor mailbox is closed in new");

        //        if let Some(ref outbox) = context.outbox {
        debug_assert!(!outbox.is_closed(), "Outbox is closed in new");
        //       }

        if let Some(ref supervisor_outbox) = context.supervisor_outbox {
            debug_assert!(
                !supervisor_outbox.is_closed(),
                "Supervisor outbox is closed in new"
            );
        }

        //        tracing::trace!("Returning new Actor with ID: {}", id);

        let actor = Actor {
            ctx: Idle::default(),
            context,
            parent_return_envelope,
            halt_signal: Default::default(),
            key,
            state,
            task_tracker,
            mailbox,
        };
        tracing::warn!("{:?}", &actor);
        actor
    }
    #[instrument(skip(self))]
    pub async fn spawn(self) -> Context {
        let mut actor = self;
        let reactors = mem::take(&mut actor.ctx.reactors);
        let context = actor.context.clone();
        let active_actor: Actor<Awake<State>, State> = actor.into();
        let mut actor = active_actor;

        let actor_tracker = &context.task_tracker.clone();
        debug_assert!(
            !actor.mailbox.is_closed(),
            "Actor mailbox is closed in spawn"
        );

        actor_tracker.spawn(async move { actor.wake(reactors).await });
        //close the trackers
        let _ = &context.supervisor_task_tracker().close();
        //let _ = &actor_tracker.close();
        context.task_tracker.close();

        context.clone()
    }

    #[instrument(skip(self, builder))]
    pub async fn spawn_with_pools(self, builder: PoolBuilder) -> Context {
        let mut actor = self;
        let reactors = mem::take(&mut actor.ctx.reactors);
        let mut context = actor.context.clone();

        let mut supervisor = builder.spawn(&context).await;
        context.supervisor_outbox = Some(supervisor.outbox.clone());
        context.supervisor_task_tracker = supervisor.task_tracker.clone();
        let active_actor: Actor<Awake<State>, State> = actor.into();
        let mut actor = active_actor;

        let actor_tracker = &context.task_tracker.clone();
        debug_assert!(
            !actor.mailbox.is_closed(),
            "Actor mailbox is closed in spawn_with_pools"
        );

        actor_tracker.spawn(async move { actor.wake(reactors).await });
        debug_assert!(
            !supervisor.mailbox.is_closed(),
            "Supervisor mailbox is closed in spawn_with_pools"
        );

        let supervisor_tracker = supervisor.task_tracker.clone();

        supervisor_tracker.spawn(async move { supervisor.wake_supervisor().await });
        //close the trackers
        supervisor_tracker.close();
        context.task_tracker.close();

        context
    }
}

impl<State: Default + Clone + Send + Debug + 'static> Actor<Awake<State>, State> {
    #[instrument(skip(self))]
    pub(crate) fn terminate(&self) {
        self.halt_signal.load(Ordering::SeqCst);
        self.halt_signal.store(true, Ordering::SeqCst);
    }
}
