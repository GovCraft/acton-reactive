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

use tracing::*;

use acton_reactive::prelude::*;
use acton_test::prelude::*;

use crate::setup::*;

mod setup;

#[acton_test]
async fn test_messaging_behavior() -> anyhow::Result<()> {
    initialize_tracing();
    let mut system: AgentRuntime = ActonApp::launch();
    let mut actor = system.new_agent::<PoolItem>().await;
    actor
        .act_on::<Ping>(|actor, event| {
            let type_name = std::any::type_name::<Ping>();
            info!(type_name = type_name, "Received in sync handler");
            actor.model.receive_count += 1;
            AgentReply::immediate()
        })
        .after_stop(|actor| {
            info!("Processed {} Pings", actor.model.receive_count);
            AgentReply::immediate()
        });
    let actor_ref = actor.start().await;
    actor_ref.send(Ping).await;
    actor_ref.stop().await?;
    Ok(())
}

#[acton_test]
async fn test_basic_messenger() -> anyhow::Result<()> {
    initialize_tracing();
    let mut system: AgentRuntime = ActonApp::launch();
    let mut actor = system.new_agent::<Messenger>().await;
    actor
        .act_on::<Ping>(|actor, event| {
            let type_name = std::any::type_name::<Ping>();
            info!(type_name = type_name, "Received in Messenger handler");
            AgentReply::immediate()
        })
        .after_stop(|actor| {
            info!("Stopping");
            AgentReply::immediate()
        });
    let actor_ref = actor.start().await;
    actor_ref.send(Ping).await;
    actor_ref.stop().await?;
    Ok(())
}

#[acton_test]
async fn test_async_messaging_behavior() -> anyhow::Result<()> {
    initialize_tracing();
    let mut system: AgentRuntime = ActonApp::launch();
    let mut actor = system.new_agent::<PoolItem>().await;
    actor
        .act_on::<Ping>(|actor, event| {
            let type_name = std::any::type_name::<Ping>();
            info!(type_name = type_name, "Received in async handler");
            actor.model.receive_count += 1;
            AgentReply::immediate()
        })
        .after_stop(|actor| {
            info!("Processed {} Pings", actor.model.receive_count);
            AgentReply::immediate()
        });
    let context = actor.start().await;
    context.send(Ping).await;
    context.stop().await?;
    Ok(())
}
