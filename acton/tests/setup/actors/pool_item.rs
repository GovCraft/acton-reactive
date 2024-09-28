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
use std::future::Future;
use std::pin::Pin;

use async_trait::async_trait;
use tracing::error;

use acton_core::prelude::*;

use crate::setup::*;

#[derive(Default, Debug, Clone)]
pub struct PoolItem {
    pub receive_count: usize, // Tracks the number of received events
}

impl PoolItem {
    // Initialize the actor with a given actor_name and parent context
    async fn initialize(&self, config: ActorConfig) -> ActorRef {
        let mut acton: SystemReady = ActonSystem::launch();

        let broker = acton.get_broker();

        let actor_config = ActorConfig::new(
            Ern::with_root("improve_show").expect("Couldn't create pool item Ern"),
            None,
            Some(broker.clone()),
        );

        let mut actor = acton.create_actor::<PoolItem>().await;
        // let mut actor = Acton::<PoolItem>::create_with_config(config.clone());

        // Log the mailbox state immediately after actor creation
        tracing::trace!(
            "Actor initialized with key: {}, mailbox closed: {}",
            actor.ern,
            actor.inbox.is_closed()
        );

        // Set up the actor to handle Ping events and define behavior before stopping
        actor
            .act_on::<Ping>(|actor, _event| {
                tracing::debug!(actor = actor.ern.to_string(), "Received Ping event for");
                actor.entity.receive_count += 1; // Increment receive_count on Ping event
                ActorRef::noop()

            })
            .before_stop_async(|actor| {
                let parent = &actor.parent.clone().unwrap();
                let final_count = actor.entity.receive_count;
                // let parent_envelope = parent.key.clone();
                let parent_address = parent.ern();
                let actor_address = actor.ern.clone();

                let parent = parent.clone();
                ActorRef::wrap_future(Self::output_results(
                    final_count,
                    parent_address.to_string(),
                    actor_address.to_string(),
                    parent,
                ))
            });

        // Activate the actor and return the context
        actor.activate().await
    }
}

impl PoolItem {
    async fn output_results(
        final_count: usize,
        parent_address: String,
        actor_address: String,
        parent: ActorRef,
    ) {
        tracing::debug!(
            "Reporting {} complete to {} from {}.",
            final_count,
            parent_address,
            actor_address,
        );
        parent.emit(StatusReport::Complete(final_count)).await
    }
}
