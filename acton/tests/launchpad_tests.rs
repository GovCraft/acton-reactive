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
use tracing::field::debug;
use tracing::*;

use acton::prelude::Subscriber;
use acton::prelude::*;
use acton_test::prelude::*;

use crate::setup::*;

mod setup;

#[acton_test]
async fn test_launch_passing_acton() -> anyhow::Result<()> {
    initialize_tracing();
    let mut acton_ready: SystemReady = ActonSystem::launch();
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

                let mut acton = acton_ready.clone();

                let child_context = acton
                    .spawn_actor_with_setup::<Parent>(child_actor_config, |mut child| {
                        Box::pin(async move {
                            child.act_on::<Pong>(|_actor, _msg| {
                                info!("CHILD SUCCESS! PONG!");
                                ActorRef::noop()

                            });

                            let child_context = &child.actor_ref.clone();
                            child_context.subscribe::<Pong>().await;
                            child.activate().await
                        })
                    })
                    .await
                    .expect("Couldn't create child actor");

                actor
                    .act_on::<Ping>(|_actor, _msg| {
                        info!("SUCCESS! PING!");
                        ActorRef::noop()

                    })
                    .act_on::<Pong>(|actor, _msg| ActorRef::wrap_future(wait_and_respond()));
                let context = &actor.actor_ref.clone();

                context.subscribe::<Ping>().await;
                context.subscribe::<Pong>().await;

                actor.activate().await
            })
        })
        .await?;

    broker_clone.emit(BrokerRequest::new(Ping)).await;
    broker_clone.emit(BrokerRequest::new(Pong)).await;

    parent_actor.suspend().await?;
    broker_clone.suspend().await?;
    Ok(())
}

async fn wait_and_respond() {
    tokio::time::sleep(Duration::from_secs(1)).await;
    info!("Waited, then...SUCCESS! PONG!");
}

#[acton_test]
async fn test_launchpad() -> anyhow::Result<()> {
    initialize_tracing();
    let mut acton_ready: SystemReady = ActonSystem::launch();

    let broker = acton_ready.get_broker();

    let actor_config =
        ActorConfig::new(Ern::with_root("improve_show")?, None, Some(broker.clone()))?;

    let comedian_actor = acton_ready
        .spawn_actor::<Comedian>(|mut actor| {
            Box::pin(async move {
                actor
                    .act_on::<Ping>(|_actor, _msg| {
                        info!("SUCCESS! PING!");
                        ActorRef::noop()
                    })
                    .act_on::<Pong>(|_actor, _msg| {
                        Box::pin(async move {
                            info!("SUCCESS! PONG!");
                        })
                    });

                actor.actor_ref.subscribe::<Ping>().await;
                actor.actor_ref.subscribe::<Pong>().await;

                actor.activate().await
            })
        })
        .await?;

    let counter_actor = acton_ready
        .spawn_actor::<Counter>(|mut actor| {
            Box::pin(async move {
                actor.act_on::<Pong>(|_actor, _msg| {
                    Box::pin(async move {
                        info!("SUCCESS! PONG!");
                    })
                });

                actor.actor_ref.subscribe::<Pong>().await;

                actor.activate().await
            })
        })
        .await?;

    broker.emit(BrokerRequest::new(Ping)).await;
    broker.emit(BrokerRequest::new(Pong)).await;

    broker.suspend().await?;
    comedian_actor.suspend().await?;
    counter_actor.suspend().await?;
    Ok(())
}
