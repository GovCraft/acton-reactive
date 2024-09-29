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
    let mut app: AgentRuntime = ActonApp::launch();
    let broker = app.get_broker();

    let actor_config = ActorConfig::new(
        Ern::with_root("improve_show").unwrap(),
        None,
        Some(broker.clone()),
    )?;

    let mut comedy_show = app.create_actor_with_config::<Comedian>(actor_config).await;

    let actor_config = ActorConfig::new(
        Ern::with_root("counter").unwrap(),
        None,
        Some(broker.clone()),
    )?;
    let mut counter_actor = app.create_actor_with_config::<Counter>(actor_config).await;
    counter_actor.act_on::<Pong>(|actor, event| {
        info!("Also SUCCESS! PONG!");
        AgentReply::immediate()

    });

    comedy_show
        .act_on::<Ping>(|agent, event| {
            info!("SUCCESS! PING!");
            AgentReply::immediate()
        })
        .act_on::<Pong>(|actor, event| {
            info!("SUCCESS! PONG!");
            AgentReply::immediate()

        });

    counter_actor.handle.subscribe::<Pong>().await;
    comedy_show.handle.subscribe::<Ping>().await;
    comedy_show.handle.subscribe::<Pong>().await;

    let _ = comedy_show.start().await;
    let _ = counter_actor.start().await;

    broker.broadcast(Ping).await;
    broker.broadcast(Pong).await;

    broker.stop().await?;
    app.shutdown_all().await?;

    Ok(())
}
