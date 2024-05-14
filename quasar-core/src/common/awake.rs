/*
 *
 *  * Copyright (c) 2024 Govcraft.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

use dashmap::mapref::one::Ref;
use std::any::TypeId;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::time::Duration;

use quasar_qrn::Qrn;
use tokio::sync::mpsc::channel;

use crate::common::*;
use crate::common::{Idle, InboundChannel, LifecycleReactor, StopSignal};
use crate::traits::QuasarMessage;
use tracing::field::debug;
use tracing::{debug, instrument, trace, warn};

pub struct Awake<State: Default + Send + Debug + 'static> {
    on_wake: Box<LifecycleReactor<Awake<State>, State>>,
    pub(crate) on_before_stop: Box<LifecycleReactor<Awake<State>, State>>,
    pub(crate) on_before_stop_async: Option<LifecycleReactorAsync<State>>,
    pub(crate) on_stop: Box<LifecycleReactor<Awake<State>, State>>,
    pub key: Qrn,
}

impl<State: Default + Send + Debug + 'static> Awake<State> {
    #[instrument(skip(actor, mailbox, reactors), fields(actor.key.value))]
    pub(crate) async fn wake(
        mut mailbox: InboundChannel,
        mut actor: Actor<Awake<State>, State>,
        reactors: ReactorMap<State>,
    ) where
        State: Send + 'static,
    {
        (actor.ctx.on_wake)(&actor);

        let mut yield_counter = 0;
        while let Some(envelope) = mailbox.recv().await {
            let type_id = envelope.message.as_any().type_id();

            if let Some(reactor) = reactors.get(&type_id) {
                match reactor.value() {
                    ReactorItem::Message(reactor) => {
                        (*reactor)(&mut actor, &envelope);
                    }
                    ReactorItem::Future(fut) => {
                        fut(&mut actor, &envelope).await;
                    }
                    _ => {}
                }
            }
            if let Some(SystemSignal::Terminate) =
                envelope.message.as_any().downcast_ref::<SystemSignal>()
            {
                actor.terminate();
            }

            let should_stop = actor.halt_signal.load(Ordering::Acquire) && mailbox.is_empty();
            if should_stop {
                (actor.ctx.on_before_stop)(&actor);
                if let Some(ref on_before_stop_async) = actor.ctx.on_before_stop_async {
                    (on_before_stop_async)(&actor).await;
                }
                break;
            }

            // Yield less frequently to reduce context switching
            yield_counter += 1;
            if yield_counter % 100 == 0 {
                tokio::task::yield_now().await;
            }
        }

        (actor.ctx.on_stop)(&actor);
    }
}

impl<State: Default + Send + Debug + 'static> From<Actor<Idle<State>, State>>
    for Actor<Awake<State>, State>
{
    #[instrument("from idle to awake", skip(value))]
    fn from(value: Actor<Idle<State>, State>) -> Actor<Awake<State>, State>
    where
        State: Send + 'static,
    {
        let on_wake = value.ctx.on_wake;
        let on_stop = Box::new(value.ctx.on_stop);
        let on_before_stop = value.ctx.on_before_stop;
        let on_before_stop_async = value.ctx.on_before_stop_async;
        let halt_signal = StopSignal::new(false);
        let parent_return_envelope = value.parent_return_envelope;
        let key = value.key.clone();
        let subordinates = value.subordinates;
        let task_tracker = value.task_tracker.clone();
        let outbox = value.ctx.outbox;
        Actor {
            ctx: Awake {
                on_wake,
                on_before_stop,
                on_before_stop_async,
                on_stop,
                key,
            },
            outbox: Some(outbox),
            parent_return_envelope,
            halt_signal,
            key: value.key,
            state: value.state,
            subordinates,
            task_tracker,
        }
    }
}
