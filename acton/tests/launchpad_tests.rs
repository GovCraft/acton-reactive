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
use std::time::Duration;

use tokio::runtime::Runtime;
use tokio::task;
use tracing::*;
use tracing::field::debug;

use acton::prelude::*;
use acton::prelude::Subscriber;
use acton_test::prelude::*;

use crate::setup::*;

mod setup;

#[acton_test]
async fn test_launch_passing_acton() -> anyhow::Result<()> {
    initialize_tracing();
    let mut acton_ready: AgentRuntime = ActonApp::launch();
    let broker = acton_ready.get_broker();

    let actor_config = ActorConfig::new(Ern::with_root("parent")?, None, Some(broker.clone()))?;

    let broker_clone = broker.clone();
    let parent_actor = acton_ready
        .clone()
        .spawn_actor_with_setup::<Parent>(actor_config, |mut actor| {
            Box::pin(async move {
                let child_actor_config = ActorConfig::new(
                    Ern::with_root("child").expect("Could not create child ARN root"),
                    None,
                    Some(broker.clone()),
                )
                    .expect("Couldn't create child config");

                let mut acton = actor.runtime.clone();

                let child_context = acton
                    .spawn_actor_with_setup::<Parent>(child_actor_config, |mut child| {
                        Box::pin(async move {
                            child.act_on::<Pong>(|_actor, _msg| {
                                info!("CHILD SUCCESS! PONG!");
                                AgentReply::immediate()
                            });

                            let child_context = &child.handle.clone();
                            child_context.subscribe::<Pong>().await;
                            child.start().await
                        })
                    })
                    .await
                    .expect("Couldn't create child actor");

                actor
                    .act_on::<Ping>(|_actor, _msg| {
                        info!("SUCCESS! PING!");
                        AgentReply::immediate()
                    })
                    .act_on::<Pong>(|actor, _msg| AgentReply::from_async(wait_and_respond()));
                let context = &actor.handle.clone();

                context.subscribe::<Ping>().await;
                context.subscribe::<Pong>().await;

                actor.start().await
            })
        })
        .await?;

    broker_clone.broadcast(Ping).await;
    broker_clone.broadcast(Pong).await;

    acton_ready.shutdown_all().await?;
    Ok(())
}

async fn wait_and_respond() {
    tokio::time::sleep(Duration::from_secs(1)).await;
    info!("Waited, then...SUCCESS! PONG!");
}

#[acton_test]
async fn test_launchpad() -> anyhow::Result<()> {
    initialize_tracing();
    let mut app: AgentRuntime = ActonApp::launch();

    let broker = app.get_broker();

    let actor_config =
        ActorConfig::new(Ern::with_root("improve_show")?, None, Some(broker.clone()))?;

    let comedian_actor = app
        .spawn_actor::<Comedian>(|mut actor| {
            Box::pin(async move {
                actor
                    .act_on::<Ping>(|_actor, _msg| {
                        info!("SUCCESS! PING!");
                        AgentReply::immediate()
                    })
                    .act_on::<Pong>(|_actor, _msg| {
                        Box::pin(async move {
                            info!("SUCCESS! PONG!");
                        })
                    });

                actor.handle.subscribe::<Ping>().await;
                actor.handle.subscribe::<Pong>().await;

                actor.start().await
            })
        })
        .await?;

    let counter_actor = app
        .spawn_actor::<Counter>(|mut actor| {
            Box::pin(async move {
                actor.act_on::<Pong>(|_actor, _msg| {
                    Box::pin(async move {
                        info!("SUCCESS! PONG!");
                    })
                });

                actor.handle.subscribe::<Pong>().await;

                actor.start().await
            })
        })
        .await?;

    assert!(app.agent_count() > 0, "No agents were spawned");

    broker.broadcast(Ping).await;
    broker.broadcast(Pong).await;

    app.shutdown_all().await?;
    Ok(())
}
