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
    async fn initialize(&self, config: AgentConfig) -> AgentHandle {
        let mut acton: AgentRuntime = ActonApp::launch();

        let broker = acton.broker();

        let actor_config = AgentConfig::new(
            Ern::with_root("improve_show").expect("Couldn't create pool item Ern"),
            None,
            Some(broker.clone()),
        );

        let mut actor = acton.new_agent::<PoolItem>().await;
        // let mut actor = Acton::<PoolItem>::create_with_config(config.clone());


        // Set up the actor to handle Ping events and define behavior before stopping
        actor
            .act_on::<Ping>(|actor, _event| {
                tracing::debug!(actor = actor.id().to_string(), "Received Ping event for");
                actor.model.receive_count += 1; // Increment receive_count on Ping event
                AgentReply::immediate()
            })
            .after_stop(|actor| {
                let parent = &actor.parent().clone().unwrap();
                let final_count = actor.model.receive_count;
                // let parent_envelope = parent.key.clone();
                let parent_address = parent.id();
                let actor_address = actor.id().clone();

                let parent = parent.clone();
                AgentReply::from_async(Self::output_results(
                    final_count,
                    parent_address.to_string(),
                    actor_address.to_string(),
                    parent,
                ))
            });

        // Activate the actor and return the context
        actor.start().await
    }
}

impl PoolItem {
    async fn output_results(
        final_count: usize,
        parent_address: String,
        actor_address: String,
        parent: AgentHandle,
    ) {
        tracing::debug!(
            "Reporting {} complete to {} from {}.",
            final_count,
            parent_address,
            actor_address,
        );
        parent.send(StatusReport::Complete(final_count)).await
    }
}
