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
#![allow(unused)]

use std::any::TypeId;
use std::pin::Pin;
use std::time::Duration;

use tracing::{debug, trace};

use akton::prelude::*;

use crate::setup::*;

mod setup;

// #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// async fn test_broker_subscription() -> anyhow::Result<()> {
//     init_tracing();
//
//
//     let mut tangle = Akton::<BrokerOwner>::create_with_id("broker_manager");
//     tangle.setup.act_on::<Pong>(|_,_|{
//        trace!("PONG");
//     });
//     let mut counter = Akton::<Counter>::create_with_id("counter");
//     counter.setup.act_on::<Ping>(|_,_|{
//        trace!("PING");
//     });
//     let counter = counter.activate(None).await?;
//     tangle.state.broker = counter.clone();
//
//     let tangle_context = tangle.activate(None).await?;
//     let error_msg = Ping;
//     // let message_type_id = TypeId::of::<ErrorNotification>();
//     // let broker_emit_msg = BrokerEmit {message: Box::new(error_msg), message_type_id };
//     debug!("Broadcasting error through broker");
//     counter.emit_async(error_msg).await?;
//
//     tangle_context.terminate().await?;
//
//     Ok(())
// }

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_async_reactor() -> anyhow::Result<()> {
    init_tracing();

    let mut comedy_show = Akton::<Comedian>::create_with_id("improve_show");

    comedy_show
        .setup
        .act_on_async::<FunnyJoke>(|actor, record| {
            actor.state.jokes_told += 1;
            let context = actor.context.clone();
            Box::pin(async move {
                debug!("emitting async");
                context.emit_async(Ping).await;
            })
        })
        .act_on::<AudienceReactionMsg>(|actor, event| {
            debug!("Rcvd AudiencReaction");
            match event.message {
                AudienceReactionMsg::Chuckle => actor.state.funny += 1,
                AudienceReactionMsg::Groan => actor.state.bombers += 1,
            };
        })
        .act_on::<Ping>(|actor, event| {
            debug!("PING");
        })
        .on_stop(|actor| {
            tracing::info!(
                "Jokes Told: {}\tFunny: {}\tBombers: {}",
                actor.state.jokes_told,
                actor.state.funny,
                actor.state.bombers
            );
            assert_eq!(actor.state.jokes_told, 2);
        });

    let comedian = comedy_show.activate(None).await?;

    comedian.emit_async(FunnyJoke::ChickenCrossesRoad).await;
    comedian.emit_async(FunnyJoke::Pun).await;
    let _ = comedian.terminate().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_lifecycle_handlers() -> anyhow::Result<()> {
    // Initialize tracing for logging purposes
    init_tracing();

    // Create an actor for counting
    let mut counter_actor = Akton::<Counter>::create();
    counter_actor
        .setup
        .act_on::<Tally>(|actor, _event| {
            tracing::info!("on tally");
            actor.state.count += 1; // Increment count on tally event
        })
        .on_stop(|actor| {
            assert_eq!(4, actor.state.count); // Ensure count is 4 when stopping
            trace!("on stopping");
        });

    // Activate the counter actor
    let counter_actor = counter_actor.activate(None).await?;

    // Emit AddCount event four times
    for _ in 0..4 {
        counter_actor.emit_async(Tally::AddCount).await;
    }

    // Create an actor for messaging
    let mut messenger_actor = Akton::<Messenger>::create();
    messenger_actor
        .setup
        .on_before_wake(|_actor| {
            trace!("*");
        })
        .on_wake(|_actor| {
            trace!("*");
        })
        .on_stop(|_actor| {
            trace!("*");
        });

    // Activate the messenger actor
    let messenger_actor = messenger_actor.activate(None).await?;

    // Terminate both actors
    counter_actor.terminate().await?;
    messenger_actor.terminate().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_child_actor() -> anyhow::Result<()> {
    // Initialize tracing for logging purposes
    init_tracing();

    // Create the parent actor
    let parent_actor = Akton::<PoolItem>::create();
    let mut child_actor = Akton::<PoolItem>::create_with_id("child");
    let child_id = "child";
    // Set up the child actor with handlers
    child_actor
        .setup
        .act_on::<Ping>(|actor, event| {
            match event.message {
                Ping => {
                    actor.state.receive_count += 1; // Increment receive_count on Ping
                }
            };
        })
        .on_before_stop(|actor| {
            tracing::info!("Processed {} PONGs", actor.state.receive_count);
            // Verify that the child actor processed 22 PINGs
            assert_eq!(
                actor.state.receive_count, 22,
                "Child actor did not process the expected number of PINGs"
            );
        });

    let child_id = child_actor.key.value.clone();
    // Activate the parent actor
    let parent_context = parent_actor.activate(None).await?;
    parent_context.supervise(child_actor).await?;
    assert_eq!(
        parent_context.children().len(),
        1,
        "Parent context missing it's child after activation"
    );
    tracing::info!("Searching all children {:?}", parent_context.children());
    let found_child = parent_context.find_child(&child_id);
    assert!(
        found_child.is_some(),
        "Couldn't find child with id {}",
        child_id
    );

    // Emit PING events to the child actor 22 times
    for _ in 0..22 {
        trace!("Emitting PING");
        // child_context.emit_pool("pool", Ping).await;
    }

    trace!("Terminating parent actor");
    parent_context.terminate().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_find_child_actor() -> anyhow::Result<()> {
    // Initialize tracing for logging purposes
    init_tracing();

    // Create the parent actor
    let mut parent_actor = Akton::<PoolItem>::create();
    parent_actor.setup.on_before_wake(|actor| {
        assert_eq!(actor.context.children().len(), 1);
    });
    // Activate the parent actor
    let parent_context = parent_actor.activate(None).await?;

    let mut child_actor = Akton::<PoolItem>::create_with_id("child");
    // Set up the child actor with handlers
    let child_id = child_actor.key.value.clone();
    // Activate the child actor
    parent_context.supervise(child_actor).await?;
    assert_eq!(
        parent_context.children().len(),
        1,
        "Parent actor missing it's child"
    );

    tracing::info!("Searching all children {:?}", parent_context.children());
    let found_child = parent_context.find_child(&child_id);
    assert!(
        found_child.is_some(),
        "Couldn't find child with id {}",
        child_id
    );

    parent_context.terminate().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_actor_mutation() -> anyhow::Result<()> {
    init_tracing();

    let mut comedy_show = Akton::<Comedian>::create();

    comedy_show
        .setup
        .act_on_async::<FunnyJoke>(|actor, record| {
            actor.state.jokes_told += 1;
            let envelope = actor.new_envelope();
            let message = record.message.clone();
            Box::pin(async move {
                if let Some(envelope) = envelope {
                    match message {
                        FunnyJoke::ChickenCrossesRoad => {
                            let _ = envelope.reply(AudienceReactionMsg::Chuckle, None);
                        }
                        FunnyJoke::Pun => {
                            let _ = envelope.reply(AudienceReactionMsg::Groan, None);
                        }
                    }
                } else {
                    // Box::pin(async {})
                }
            })
        })
        .act_on::<AudienceReactionMsg>(|actor, event| {
            match event.message {
                AudienceReactionMsg::Chuckle => actor.state.funny += 1,
                AudienceReactionMsg::Groan => actor.state.bombers += 1,
            };
        })
        .on_stop(|actor| {
            tracing::info!(
                "Jokes Told: {}\tFunny: {}\tBombers: {}",
                actor.state.jokes_told,
                actor.state.funny,
                actor.state.bombers
            );
            assert_eq!(actor.state.jokes_told, 2);
        });

    let comedian = comedy_show.activate(None).await?;

    comedian.emit_async(FunnyJoke::ChickenCrossesRoad).await;
    comedian.emit_async(FunnyJoke::Pun).await;
    let _ = comedian.terminate().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_child_count_in_reactor() -> anyhow::Result<()> {
    init_tracing();

    let mut comedy_show = Akton::<Comedian>::create();
    comedy_show
        .setup
        .act_on::<FunnyJokeFor>(|actor, event_record| {
            // assert_eq!(actor.context.children().len(), 1);
            match &event_record.message {
                FunnyJokeFor::ChickenCrossesRoad(child_id) => {
                    tracing::info!("got a funny joke for {}", &child_id);

                    let context = actor.context.find_child(&child_id).clone();
                    if let Some(context) = context {
                        trace!("pinging child");
                        context.emit(Ping);
                    } else {
                        tracing::error!("no child");
                    }
                }
                FunnyJokeFor::Pun(_) => {}
            }
        });
    let mut child = Akton::<Counter>::create_with_id("child");
    child.setup.act_on::<Ping>(|actor, event| {
        tracing::info!("Received Ping from parent actor");
    });
    let child_id = child.key.value.clone();
    comedy_show.context.supervise(child).await?;
    let comedian = comedy_show.activate(None).await?;
    assert_eq!(comedian.children().len(), 1);
    comedian
        .emit_async(FunnyJokeFor::ChickenCrossesRoad(child_id))
        .await;
    comedian.terminate().await?;

    Ok(())
}
