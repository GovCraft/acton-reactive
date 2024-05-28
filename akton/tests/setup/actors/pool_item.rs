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
use async_trait::async_trait;
use akton_core::prelude::*;
use crate::setup::*;

#[derive(Default, Debug, Clone)]
pub struct PoolItem {
    pub receive_count: usize, // Tracks the number of received events
}

#[async_trait]
impl PooledActor for PoolItem {
    // Initialize the actor with a given name and parent context
    async fn initialize(&self, actor_name: String, parent_context: &Context) -> Context {
        // Uncomment for debugging: tracing::trace!("Initializing actor with name: {}", actor_name);

        // Create a supervised actor
        let mut actor = parent_context.supervise::<PoolItem>(&actor_name);

        // Log the mailbox state immediately after actor creation
        tracing::trace!(
            "Actor initialized with key: {}, mailbox closed: {}",
            actor.key.value,
            actor.mailbox.is_closed()
        );

        // Set up the actor to handle Ping events and define behavior before stopping
        actor
            .setup
            .act_on::<Ping>(|actor, _event| {
                tracing::trace!(
                    "Received Ping event for actor with key: {}",
                    actor.key.value
                );
                actor.state.receive_count += 1; // Increment receive_count on Ping event
            })
            .on_before_stop(|actor| {
                let final_count = actor.state.receive_count;

                let parent_envelope = actor.new_parent_envelope();
                let parent_address = parent_envelope.sender.value.clone();
                let actor_address = actor.key.value.clone();

                tracing::info!(
                    "Reporting {} complete to {} from {}.",
                    final_count,
                    parent_address,
                    actor_address,
                );
                let _ = parent_envelope.reply(StatusReport::Complete(final_count), None);
            });

        // Activate the actor and return the context
        let actor_context = actor.activate(None).await;

        actor_context
    }
}

