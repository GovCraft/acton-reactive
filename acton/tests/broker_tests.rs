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
use std::sync::Arc;

use tracing::*;

use acton::prelude::*;
use acton_test::prelude::*;

use crate::setup::*;

mod setup;

#[acton_test]
async fn test_broker() -> anyhow::Result<()> {
    initialize_tracing();
    let mut acton: SystemReady = ActonSystem::launch().into();
    let broker = acton.get_broker();

    let actor_config = ActorConfig::new(
        Ern::with_root("improve_show").unwrap(),
        None,
        Some(broker.clone()),
    )?;

    let mut comedy_show = acton.create_actor_with_config::<Comedian>(actor_config).await;

    let actor_config = ActorConfig::new(
        Ern::with_root("counter").unwrap(),
        None,
        Some(broker.clone()),
    )?;
    let mut counter_actor = acton.create_actor_with_config::<Counter>(actor_config).await;
    counter_actor.act_on::<Pong>(|actor, event| {
        info!("Also SUCCESS! PONG!");
    });

    comedy_show
        .act_on_async::<Ping>(|actor, event| {
            info!("SUCCESS! PING!");
            Box::pin(async move {})
        })
        .act_on::<Pong>(|actor, event| {
            info!("SUCCESS! PONG!");
        });

    counter_actor.actor_ref.subscribe::<Pong>().await;
    comedy_show.actor_ref.subscribe::<Ping>().await;
    comedy_show.actor_ref.subscribe::<Pong>().await;

    let comedian = comedy_show.activate().await;
    let counter = counter_actor.activate().await;

    broker.emit(BrokerRequest::new(Ping)).await;
    broker.emit(BrokerRequest::new(Pong)).await;

    let _ = comedian.suspend().await?;
    let _ = counter.suspend().await?;
    let _ = broker.suspend().await?;

    Ok(())
}
