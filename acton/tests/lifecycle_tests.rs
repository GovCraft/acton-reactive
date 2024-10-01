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
use acton::prelude::*;
use acton_test::prelude::*;

use crate::setup::*;

mod setup;

#[acton_test]
async fn test_actor_lifecycle_events() -> anyhow::Result<()> {
    initialize_tracing();
    let mut acton_ready: AgentRuntime = ActonApp::launch();
    let mut pool_item_actor = acton_ready.new_agent::<PoolItem>().await;

    pool_item_actor
        .after_start(|actor| {
            tracing::info!("Actor woke up with key: {}", actor.id());
            AgentReply::immediate()

        })
        .after_stop(|actor| {
            tracing::info!("Actor stopping with key: {}", actor.id());
            AgentReply::immediate()
        });

    let actor_context = pool_item_actor.start().await;
    actor_context.stop().await?;
    Ok(())
}
