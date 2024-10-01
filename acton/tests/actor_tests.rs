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
use std::pin::Pin;
use std::time::Duration;

use tracing::{debug, info, trace};
use tracing_subscriber::fmt::writer::EitherWriter::A;

use acton::prelude::*;
use acton_test::prelude::*;

use crate::setup::*;

mod setup;

#[acton_test]
async fn test_async_reactor() -> anyhow::Result<()> {
    initialize_tracing();

    let mut acton: AgentRuntime = ActonApp::launch();

    let actor_config = ActorConfig::new(Ern::with_root("improve_show").unwrap(), None, None)?;
    let mut comedy_show = acton.create_actor_with_config::<Comedian>(actor_config).await;

    comedy_show
        .act_on::<FunnyJoke>(|actor, record| {
            actor.model.jokes_told += 1;
            let context = actor.handle().clone();
            Box::pin(async move {
                trace!("emitting async");
                context.send(Ping).await;
            })
        })
        .act_on::<AudienceReactionMsg>(|actor, event| {
            trace!("Received Audience Reaction");
            match event.message(){
                AudienceReactionMsg::Chuckle => actor.model.funny += 1,
                AudienceReactionMsg::Groan => actor.model.bombers += 1,
            };
            AgentReply::immediate()
        })
        .act_on::<Ping>(|actor, event| {
            trace!("PING");
            AgentReply::immediate()
        })
        .after_stop(|actor| {
            info!(
                "Jokes told at {}: {}\tFunny: {}\tBombers: {}",
                actor.id(),
                actor.model.jokes_told,
                actor.model.funny,
                actor.model.bombers
            );
            assert_eq!(actor.model.jokes_told, 2);
            AgentReply::immediate()

        });

    let comedian = comedy_show.start().await;

    comedian
        .send(FunnyJoke::ChickenCrossesRoad)
        .await;
    comedian.send(FunnyJoke::Pun).await;
    comedian.stop().await?;

    Ok(())
}

#[acton_test]
async fn test_lifecycle_handlers() -> anyhow::Result<()> {
    // Initialize tracing for logging purposes
    initialize_tracing();

    let mut acton: AgentRuntime = ActonApp::launch();
    // Create an actor for counting
    let mut counter_actor = acton.new_agent::<Counter>().await;
    counter_actor
        .act_on::<Tally>(|actor, _event| {
            info!("on tally");
            actor.model.count += 1; // Increment count on tally event
            AgentReply::immediate()
        }
        )
        .after_stop(|actor| {
            assert_eq!(4, actor.model.count); // Ensure count is 4 when stopping
            trace!("on stopping");
            AgentReply::immediate()

        });

    // Activate the counter actor
    let counter_actor = counter_actor.start().await;

    // Emit AddCount event four times
    for _ in 0..4 {
        counter_actor.send(Tally::AddCount).await;
    }

    // Create an actor for messaging
    let mut messenger_actor = acton.new_agent::<Messenger>().await;
    messenger_actor
        .after_start(|actor| {
            trace!("*");
            AgentReply::immediate()
        })
        .after_stop(|_actor| {
            trace!("*");
            AgentReply::immediate()
        });

    // Activate the messenger actor
    let messenger_actor = messenger_actor.start().await;

    // Terminate both actor
    counter_actor.stop().await?;
    messenger_actor.stop().await?;

    Ok(())
}

#[acton_test]
async fn test_child_actor() -> anyhow::Result<()> {
    // Initialize tracing for logging purposes
    initialize_tracing();
    let mut acton: AgentRuntime = ActonApp::launch();

    let actor_config = ActorConfig::new(Ern::with_root("test_child_actor").unwrap(), None, None)?;

    // Create the parent actor
    let parent_actor = acton.create_actor_with_config::<PoolItem>(actor_config).await;

    let actor_config = ActorConfig::new(
        Ern::with_root("test_child_actor_chile").unwrap(),
        None,
        None,
    )?;

    let mut child_actor = acton.create_actor_with_config::<PoolItem>(actor_config).await;

    // Set up the child actor with handlers
    child_actor
        .act_on::<Ping>(|actor, event| {
            match event.message(){
                Ping => {
                    actor.model.receive_count += 1; // Increment receive_count on Ping
                }
            };
            AgentReply::immediate()
        })
        .after_stop(|actor| {
            info!("Processed {} PONGs", actor.model.receive_count);
            // Verify that the child actor processed 22 PINGs
            assert_eq!(
                actor.model.receive_count, 22,
                "Child actor did not process the expected number of PINGs"
            );
            AgentReply::immediate()
        });

    let child_id = child_actor.id().clone();
    // Activate the parent actor
    let parent_context = parent_actor.start().await;
    parent_context.supervise(child_actor).await?;
    assert_eq!(
        parent_context.children().len(),
        1,
        "Parent context missing it's child after activation"
    );
    info!(child = &child_id.to_string(), "Searching all children for");
    let found_child = parent_context.find_child(&child_id);
    assert!(
        found_child.is_some(),
        "Couldn't find child with id {}",
        child_id
    );
    let child = found_child.unwrap();

    // Emit PING events to the child actor 22 times
    for _ in 0..22 {
        trace!("Emitting PING");
        child.send(Ping).await;
    }

    trace!("Terminating parent actor");
    parent_context.stop().await?;

    Ok(())
}

#[acton_test]
async fn test_find_child_actor() -> anyhow::Result<()> {
    // Initialize tracing for logging purposes
    initialize_tracing();
    let mut acton: AgentRuntime = ActonApp::launch();
    // Create the parent actor
    let mut parent_actor = acton.new_agent::<PoolItem>().await;
    // Activate the parent actor
    let parent_context = parent_actor.start().await;

    let actor_config =
        ActorConfig::new(Ern::with_root("test_find_child_actor").unwrap(), None, None)?;

    let mut child_actor = acton.create_actor_with_config::<PoolItem>(actor_config).await;
    // Set up the child actor with handlers
    let child_id = child_actor.id().clone();
    // Activate the child actor
    parent_context.supervise(child_actor).await?;
    assert_eq!(
        parent_context.children().len(),
        1,
        "Parent actor missing it's child"
    );
    info!(child = &child_id.to_string(), "Searching all children for");
    let found_child = parent_context.find_child(&child_id);
    assert!(
        found_child.is_some(),
        "Couldn't find child with id {}",
        child_id
    );
    let child = found_child.unwrap();

    parent_context.stop().await?;

    Ok(())
}

#[acton_test]
async fn test_actor_mutation() -> anyhow::Result<()> {
    initialize_tracing();
    let mut acton: AgentRuntime = ActonApp::launch();
    let actor_config =
        ActorConfig::new(Ern::with_root("test_actor_mutation").unwrap(), None, None)?;

    let mut comedy_show = acton.create_actor_with_config::<Comedian>(actor_config).await;

    comedy_show
        .act_on::<FunnyJoke>(|actor, record| {
            actor.model.jokes_told += 1;
            let envelope = actor.new_envelope();
            let message = record.message().clone();
            Box::pin(async move {
                if let Some(envelope) = envelope {
                    match message {
                        FunnyJoke::ChickenCrossesRoad => {
                            let _ = envelope
                                .send(AudienceReactionMsg::Chuckle)
                                .await;
                        }
                        FunnyJoke::Pun => {
                            let _ = envelope.send(AudienceReactionMsg::Groan).await;
                        }
                    }
                }
            })
        })
        .act_on::<AudienceReactionMsg>(|actor, event| {
            match event.message(){
                AudienceReactionMsg::Chuckle => actor.model.funny += 1,
                AudienceReactionMsg::Groan => actor.model.bombers += 1,
            };
            AgentReply::immediate()
        })
        .after_stop(|actor| {
            info!(
                "Jokes told at {}: {}\tFunny: {}\tBombers: {}",
                actor.id(),
                actor.model.jokes_told,
                actor.model.funny,
                actor.model.bombers
            );
            assert_eq!(actor.model.jokes_told, 2);
            AgentReply::immediate()
        });

    let comedian = comedy_show.start().await;

    comedian
        .send(FunnyJoke::ChickenCrossesRoad)
        .await;
    comedian.send(FunnyJoke::Pun).await;
    comedian.stop().await?;

    Ok(())
}

#[acton_test]
async fn test_child_count_in_reactor() -> anyhow::Result<()> {
    initialize_tracing();

    // Launch the Acton system and await its readiness
    let mut acton: AgentRuntime = ActonApp::launch();


    // Asynchronously create the Comedian actor and await its creation
    let mut comedy_show = acton.new_agent::<Comedian>().await;

    // Define the message handler for FunnyJokeFor messages
    comedy_show.act_on::<FunnyJokeFor>(|actor, event_record| {
        if let FunnyJokeFor::ChickenCrossesRoad(child_id) = event_record.message().clone() {
            info!("Got a funny joke for {}", &child_id);

            // Attempt to find the child actor by its ID
            assert_eq!(actor.handle().children().len(), 1, "Parent actor missing any children");
            trace!( "Parent actor has child with ID: {:?}", actor.handle().children());
            assert!(actor.handle().find_child(&child_id).is_some(), "No child found with ID {}", &child_id);
            return if let Some(context) = actor.handle().find_child(&child_id) {
                trace!("Pinging child {}", &child_id);
                // Emit a Ping message to the child actor
                let context = context.clone();
                AgentReply::from_async(async move {context.send(Ping).await})
            } else {
                tracing::error!("No child found with ID {}", &child_id);
                AgentReply::immediate()
            }
        }
        AgentReply::immediate()
    });

    // Define the actor configuration for the Child (Counter) actor
    let child_actor_config = ActorConfig::new(Ern::with_root("child").unwrap(), None, None)?;

    // Asynchronously create the Counter actor with the specified configuration and await its creation
    let mut child = acton.create_actor_with_config::<Counter>(child_actor_config).await;
    info!("Created child actor with id: {}", child.id());

    // Define the message handler for Ping messages in the Child actor
    child.act_on::<Ping>(|actor, _event| {
        info!("Received Ping from parent actor");
        AgentReply::immediate()
    });

    // Clone the child actor's key for later use
    let child_id = child.id().clone();

    // Supervise the Child actor under the Comedian actor and await the supervision process
    comedy_show.handle().supervise(child).await?;
    assert_eq!(comedy_show.handle().children().len(), 1, "Parent actor missing it's child after activation");
    // Activate the Comedian actor and await its activation
    let comedian = comedy_show.start().await;

    // Assert that the Comedian actor has exactly one child
    assert_eq!(comedian.children().len(), 1);

    // Emit a FunnyJokeFor::ChickenCrossesRoad message to the Comedian actor and await the emission
    comedian
        .send(FunnyJokeFor::ChickenCrossesRoad(child_id))
        .await;

    // Suspend the Comedian actor and await the suspension process
    comedian.stop().await?;

    Ok(())
}
