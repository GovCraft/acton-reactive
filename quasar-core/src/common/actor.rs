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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use futures::future;
use quasar_qrn::Qrn;
use tokio::sync::Mutex;
use tokio_util::task::TaskTracker;
use crate::common::{SystemSignal, Context, Idle, Awake, EventRecord};
use tracing::{error, info, instrument, trace, warn};
use crate::traits::SystemMessage;

pub struct Actor<S> {
    pub state: S,
}


impl<T: Default + Send + Sync, U: Send + Sync> Actor<Idle<T, U>> {
    pub(crate) fn new(qrn: Qrn, state: T) -> Self {
        Actor {
            state: Idle::new(qrn, state)
        }
    }
    #[instrument(skip(idle_actor))]
    // Modified Rust function to avoid the E0499 error by preventing simultaneous mutable borrows of actor.ctx
    pub async fn spawn(idle_actor: Actor<Idle<T, U>>) -> Context {

        // Convert the actor from MyActorIdle to MyActorRunning
        let mut actor = idle_actor;

        // Handle any pre_start activities
        (actor.state.on_before_wake)(&actor.state);

        // Ensure reactors are correctly assigned
        let actor = Self::assign_internal_signal_reactors(actor).await;

        trace!("Idle, message_reactor_map size: {}", &actor.state.message_reactors.len());
        // Convert Actor<Idle<T, U>> to Actor<Awake<T, U>> first
        let active_actor_awake: Actor<Awake<T, U>> = actor.into();

        // Then wrap the state (Awake<T, U>) into Arc<Mutex<_>> for shared access
        let active_actor: Arc<Mutex<Awake<T, U>>> = Arc::new(Mutex::new(active_actor_awake.state));

        // Acquire the lock to access the shared state.
        let mut active_actor_guard = active_actor.lock().await;

        let signal_reactor_map = active_actor_guard.signal_reactors.take().expect("No lifecycle reactors provided. This should never happen");
        let message_reactor_map = active_actor_guard.message_reactors.take().expect("No actor message reactors provided. This should never happen");
        trace!("message_reactor_map size before wake {}", message_reactor_map.len());
        // trace!("Actor reactor map size: {}", message_reactor_map.len());

        let actor_inbox_address = active_actor_guard.outbox.clone();
        assert!(!actor_inbox_address.is_closed(), "Actor inbox address must be valid");

        let lifecycle_inbox_address = active_actor_guard.signal_outbox.clone();
        assert!(!lifecycle_inbox_address.is_closed(), "Lifecycle inbox address must be valid");

        let qrn = active_actor_guard.key.clone();

// Drop the mutex guard as we no longer need to access the shared state directly
        drop(active_actor_guard);

        let task_tracker = TaskTracker::new();

        let _ = tokio::spawn(async move {
            Awake::<T, U>::wake(active_actor, message_reactor_map, signal_reactor_map).await;
        });

        task_tracker.close();
        assert!(task_tracker.is_closed(), "Task tracker must be closed after operations");

// Create a new QuasarContext with pre-extracted data
        Context {
            outbox: actor_inbox_address,
            signal_outbox: lifecycle_inbox_address,
            task_tracker,
            key: qrn,
        }
    }

    #[instrument(skip(actor), fields(qrn = actor.state.key.value))]
    async fn assign_internal_signal_reactors(mut actor: Actor<Idle<T, U>>) -> Actor<Idle<T, U>> {
        trace!("assigning internal signal reactors");

        actor.state.act_on_internal_signal::<SystemSignal>(Box::new(|actor: Arc<Mutex<Awake<T, U>>>, message: &dyn SystemMessage| {
            if let Some(event) = message.as_any().downcast_ref::<SystemSignal>() {
                let event_cloned = event.clone();
                let actor = actor.clone();
                Box::pin(async move {
                    // Asynchronously acquire the lock to the actor's state
                    let mut actor_guard = actor.lock().await;
                    match event_cloned {
                        SystemSignal::Terminate => {
                            trace!("Received terminate message");
                            actor_guard.terminate();
                        }
                        SystemSignal::Recreate => { todo!() }
                        SystemSignal::Suspend => { todo!() }
                        SystemSignal::Resume => { todo!() }
                        SystemSignal::Supervise => { todo!() }
                        SystemSignal::Watch => { todo!() }
                        SystemSignal::Unwatch => { todo!() }
                        SystemSignal::Failed => { todo!() }
                        SystemSignal::Wake => { todo!() }
                    }
                }) as Pin<Box<dyn Future<Output=()> + Send + Sync>>
            } else {
                error!("SystemMessage type mismatch: expected {:?}", std::any::type_name::<SystemSignal>());
                Box::pin(future::ready(()))  // Handle the type mismatch with a no-op future
            }
        }));

        actor
    }
}
