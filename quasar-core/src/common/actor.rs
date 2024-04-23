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

use quasar_qrn::Qrn;
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
        let mut active_quasar: Actor<Awake<T, U>> = actor.into();

        // Take reactor maps and inbox addresses before entering async context
        let singularity_signal_responder_map = active_quasar.state.signal_reactors.take().expect("No lifecycle reactors provided. This should never happen");
        let photon_responder_map = active_quasar.state.message_reactors.take().expect("No actor message reactors provided. This should never happen");
        trace!("Actor reactor map size: {}", photon_responder_map.len());

        let actor_inbox_address = active_quasar.state.outbox.clone();
        assert!(!actor_inbox_address.is_closed(), "Actor inbox address must be valid");

        let lifecycle_inbox_address = active_quasar.state.signal_outbox.clone();
        assert!(!lifecycle_inbox_address.is_closed(), "Lifecycle inbox address must be valid");

        let qrn = active_quasar.state.key.clone();

        let task_tracker = TaskTracker::new();

        // Spawn task to listen to messages
        // task_tracker.spawn(async move {
            active_quasar.state.wake(photon_responder_map, singularity_signal_responder_map).await;
        // });
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
                SystemSignal::Wake => {todo!()}
            }
        });
    }
}
