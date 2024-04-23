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

use std::sync::Arc;
use quasar_qrn::Qrn;
use tokio::sync::Mutex;
use tokio_util::task::TaskTracker;
use crate::common::{SystemSignal, Context, Idle, Awake};
use tracing::{instrument, trace, warn};

pub struct Actor<S> {
    pub state: S,
}


impl<T: Default + Send + Sync, U: Send + Sync> Actor<Idle<T, U>> {
    pub(crate) fn new(qrn: Qrn, state: T) -> Self {
        Actor {
            state: Idle::new(qrn, state)
        }
    }
    #[instrument(skip(dormant_quasar))]
    // Modified Rust function to avoid the E0499 error by preventing simultaneous mutable borrows of actor.ctx
    pub async fn spawn(dormant_quasar: Actor<Idle<T, U>>) -> Context {

        // Convert the actor from MyActorIdle to MyActorRunning
        let mut actor = dormant_quasar;

        // Handle any pre_start activities
        (actor.state.on_before_wake)(&actor.state);

        // Ensure reactors are correctly assigned
        Self::assign_lifecycle_reactors(&mut actor);

        // Convert to QuasarRunning state
        // Convert Actor<Idle<T, U>> to Actor<Awake<T, U>> first
        let active_actor_awake: Actor<Awake<T, U>> = actor.into();

        // Then wrap the state (Awake<T, U>) into Arc<Mutex<_>> for shared access
        let active_actor: Arc<Mutex<Awake<T, U>>> = Arc::new(Mutex::new(active_actor_awake.state));

        // Acquire the lock to access the shared state.
        let mut active_actor_guard = active_actor.lock().await;

        let singularity_signal_responder_map = active_actor_guard.signal_reactors.take().expect("No lifecycle reactors provided. This should never happen");
        let photon_responder_map = active_actor_guard.message_reactors.take().expect("No actor message reactors provided. This should never happen");
        trace!("Actor reactor map size: {}", photon_responder_map.len());

        let actor_inbox_address = active_actor_guard.outbox.clone();
        assert!(!actor_inbox_address.is_closed(), "Actor inbox address must be valid");

        let lifecycle_inbox_address = active_actor_guard.signal_outbox.clone();
        assert!(!lifecycle_inbox_address.is_closed(), "Lifecycle inbox address must be valid");

        let qrn = active_actor_guard.key.clone();

// Drop the mutex guard as we no longer need to access the shared state directly
        drop(active_actor_guard);

        let task_tracker = TaskTracker::new();

// Assume `active_actor` is an `Arc<Mutex<Awake<T, U>>>`
//         let mut actor_guard = active_actor.lock().await;

// Now call `wake` on the actual `Awake<T, U>` reference
//         actor_guard.wake(photon_responder_map, singularity_signal_responder_map).await;
// Correctly use the associated function, passing `&mut` reference to the `Awake<T, U>`
        Awake::<T, U>::wake(active_actor, photon_responder_map, singularity_signal_responder_map).await;

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
    fn assign_lifecycle_reactors(actor: &mut Actor<Idle<T, U>>) {
        trace!("assigning_lifeccycle reactors");

        actor.state.observe_singularity_signal::<SystemSignal>(|awake_actor, lifecycle_message| {
            match lifecycle_message {
                SystemSignal::Terminate => {
                    trace!("Received terminate message");
                    awake_actor.terminate();
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
        });
    }
}
