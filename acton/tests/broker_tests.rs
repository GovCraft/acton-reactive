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
    let broker = app.broker();


    let mut comedy_show = app.new_agent::<Comedian>().await;

    let actor_config = ActorConfig::new(
        Ern::with_root("counter").unwrap(),
        None,
        Some(broker.clone()),
    )?;
    let mut counter_actor = app.create_actor_with_config::<Counter>(actor_config).await;
    counter_actor.act_on::<Pong>(|agent, context| {
        info!("Also SUCCESS! PONG!");
        agent.model.count += 1;

        AgentReply::immediate()
    }).after_stop(|agent| {
        assert_eq!(agent.model.count, 1, "count should be 1");
        AgentReply::immediate()
    });

    comedy_show
        .act_on::<Ping>(|agent, event| {
            info!("SUCCESS! PING!");
            agent.model.funny += 1;
            AgentReply::immediate()
        })
        .act_on::<Pong>(|agent, event| {
            agent.model.funny += 1;
            info!("SUCCESS! PONG!");
            AgentReply::immediate()
        })
        .after_stop(|agent| {
            assert_eq!(agent.model.funny, 2, "funny count should be 2");
            AgentReply::immediate()
        });

    counter_actor.handle().subscribe::<Pong>().await;
    comedy_show.handle().subscribe::<Ping>().await;
    comedy_show.handle().subscribe::<Pong>().await;

    let agent = comedy_show.start().await;
    let _ = counter_actor.start().await;

    if let Some(broker) = agent.broker.as_ref() {
        broker.broadcast(Ping).await;
        broker.broadcast(Pong).await;
    }

    app.shutdown_all().await?;

    Ok(())
}

#[acton_test]
async fn test_broker_from_handler() -> anyhow::Result<()> {
    initialize_tracing();
    let mut app: AgentRuntime = ActonApp::launch();
    let broker = app.broker();


    let mut comedy_show = app.new_agent::<Comedian>().await;

    let actor_config = ActorConfig::new(
        Ern::with_root("counter").unwrap(),
        None,
        Some(broker.clone()),
    )?;
    let mut counter_actor = app.create_actor_with_config::<Counter>(actor_config).await;
    counter_actor.act_on::<Pong>(|agent, context| {
        info!("Also SUCCESS! PONG!");
        agent.model.count += 1;

        AgentReply::immediate()
    }).after_stop(|agent| {
        assert_eq!(agent.model.count, 1, "count should be 1");
        AgentReply::immediate()
    });

    comedy_show
        .act_on::<Ping>(|agent, event| {
            let broker = agent.broker().clone();
            Box::pin(async move {
                broker.broadcast(Pong).await;
            })
        });

    counter_actor.handle().subscribe::<Pong>().await;
    comedy_show.handle().subscribe::<Ping>().await;

    let _ = comedy_show.start().await;
    let _ = counter_actor.start().await;

    broker.broadcast(Ping).await;

    app.shutdown_all().await?;

    Ok(())
}
