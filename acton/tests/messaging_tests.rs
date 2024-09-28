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

use acton::prelude::*;
use acton_test::prelude::*;

use crate::setup::*;

mod setup;

#[acton_test]
async fn test_messaging_behavior() -> anyhow::Result<()> {
    initialize_tracing();
    let mut system: SystemReady = ActonSystem::launch();
    let mut actor = system.create_actor::<PoolItem>().await;
    actor
        .act_on::<Ping>(|actor, event| {
            let type_name = std::any::type_name::<Ping>();
            info!(type_name = type_name, "Received in sync handler");
            actor.entity.receive_count += 1;
            ActorRef::noop()

        })
        .before_stop(|actor| {
            info!("Processed {} Pings", actor.entity.receive_count);
        });
    let actor_ref = actor.activate().await;
    actor_ref.emit(Ping).await;
    actor_ref.suspend().await?;
    Ok(())
}
#[acton_test]
async fn test_basic_messenger() -> anyhow::Result<()> {
    initialize_tracing();
    let mut system: SystemReady = ActonSystem::launch();
    let mut actor = system.create_actor::<Messenger>().await;
    actor
        .act_on::<Ping>(|actor, event| {
            let type_name = std::any::type_name::<Ping>();
            info!(type_name = type_name, "Received in Messenger handler");
            ActorRef::noop()
        })
        .before_stop(|actor| {
            info!("Stopping");
        });
    let actor_ref = actor.activate().await;
    actor_ref.emit(Ping).await;
    actor_ref.suspend().await?;
    Ok(())
}

#[acton_test]
async fn test_async_messaging_behavior() -> anyhow::Result<()> {
    initialize_tracing();
    let mut system: SystemReady = ActonSystem::launch();
    let mut actor = system.create_actor::<PoolItem>().await;
    actor
        .act_on::<Ping>(|actor, event| {
            let type_name = std::any::type_name::<Ping>();
            info!(type_name = type_name, "Received in async handler");
            actor.entity.receive_count += 1;
            ActorRef::noop()
        })
        .before_stop(|actor| {
            info!("Processed {} Pings", actor.entity.receive_count);
        });
    let context = actor.activate().await;
    context.emit(Ping).await;
    context.suspend().await?;
    Ok(())
}
