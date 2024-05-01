/*
 *
 *  * Copyright (c) 2024 Govcraft.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */






use std::time::Duration;
use quasar::prelude::async_trait::async_trait;
use quasar_core::prelude::*;
use quasar::prelude::*;
use tracing::{debug, info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Default, Debug)]
pub struct Comedian {
    pub jokes_told: usize,
    pub funny: usize,
    pub bombers: usize,
}

#[tokio::test]
async fn test_actor_mutation() -> anyhow::Result<()> {
    // console_subscriber::init();
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .compact()
        .with_line_number(true)
        .without_time()
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let mut comedy_show = System::new_actor(Comedian::default());

    comedy_show.ctx.act_on_async::<FunnyJoke>(|actor, record: &EventRecord<&FunnyJoke>| {
        actor.state.jokes_told += 1;

        if let Some(envelope) = actor.new_envelope() {
            match record.message {
                FunnyJoke::ChickenCrossesRoad => {
                    Box::pin(async move {
                        envelope.reply(AudienceReaction::Chuckle).await.expect("TODO: panic message");
                    })
                }
                FunnyJoke::Pun => {
                    Box::pin(async move {
                        envelope.reply(AudienceReaction::Groan).await.expect("TODO: panic message");
                    })
                }
            }
        } else {
            Box::pin(async {})
        }
    })
        .act_on::<AudienceReaction>(|actor, event| {
            match event.message {
                AudienceReaction::Chuckle => { actor.state.funny += 1 }
                AudienceReaction::Groan => { actor.state.bombers += 1 }
            }
        })
        .on_stop(|actor| {
            debug!("Jokes Told: {}\tFunny: {}\tBombers: {}", actor.state.jokes_told,actor.state.funny, actor.state.bombers);
        });

    let comedian = comedy_show.spawn().await;

    // let mut comedian = Actor::spawn(joke_counter).await;
    comedian.emit(FunnyJoke::ChickenCrossesRoad).await?;
    comedian.emit(FunnyJoke::Pun).await?;
    comedian.terminate().await.expect("TODO: panic message");
    Ok(())
}

#[derive(Default, Debug)]
pub struct Counter {
    pub count: usize,
}

#[derive(Default, Debug)]
pub struct Messenger {
    pub sender: Option<OutboundEnvelope>,
}

#[tokio::test]
async fn test_lifecycle_handlers() -> anyhow::Result<()> {
    let counter = Counter {
        count: 0,
    };

    let mut count = System::new_actor(counter);
    count.ctx.act_on::<Tally>(|actor, _event| {
        actor.state.count += 1;
    }).on_stop(|actor| {
        assert_eq!(4, actor.state.count);
        debug_assert!(false);
    });
    let count = count.spawn().await;


    let actor = Messenger {
        sender: Some(count.return_address()),
    };
    let mut actor = System::new_actor(actor);
    actor.ctx
        .on_before_wake(|actor| {
            if let Some(envelope) = actor.state.sender.clone() {
                tokio::spawn(async move {
                    envelope.reply(Tally::AddCount).await
                });
            }
        })
        .on_wake(|actor| {
            if let Some(envelope) = actor.state.sender.clone() {
                tokio::spawn(async move {
                    envelope.reply(Tally::AddCount).await
                });
            }
        })
        .on_stop(|actor| {
            if let Some(envelope) = actor.state.sender.clone() {
                tokio::spawn(async move {
                    envelope.reply(Tally::AddCount).await
                });
            }
        });

    let actor = actor.spawn().await;

    actor.terminate().await?;
    count.terminate().await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}

#[derive(Default, Debug)]
pub struct PoolItem;

#[async_trait]
impl ConfigurableActor for PoolItem {
    async fn init(name: String, context: &Context) -> Context {
        let mut actor = context.new_actor::<PoolItem>(&name);
        actor.ctx
            .act_on::<Pong>(|actor, _| {
                info!("{} PONG!", actor.key.value);
            });

        actor.spawn().await
    }
}

#[tokio::test]
async fn test_actor_pool() -> anyhow::Result<()> {
    // let subscriber = FmtSubscriber::builder()
    //     .with_max_level(Level::DEBUG)
    //     .compact()
    //     .with_line_number(true)
    //     // .without_time()
    //     .finish();
    //
    // tracing::subscriber::set_global_default(subscriber)
    //     .expect("setting default subscriber failed");

    let actor = System::new_actor(PoolItem);
    let mut context = actor.spawn().await;

    // Execute `spawn_pool`, then `terminate`
    context.spawn_pool::<PoolItem>("pool", 10).await?;
    for _ in 0..20 {
        context.pool_emit::<DistributionStrategy>("pool", FunnyJoke::Pun).await?;
    }
    context.terminate().await?;

    Ok(())
}

#[derive(Default, Debug)]
pub struct ContextWrapper {
    pub wrapped: Option<Context>,
}

#[tokio::test]
async fn test_context_wrapper() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .compact()
        .with_line_number(true)
        .without_time()
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let actor = System::new_actor(PoolItem);

    let mut context = actor.spawn().await;

    // create a pool of PoolItems`
    context.spawn_pool::<PoolItem>("pool", 10).await?;

    //TODO: make on_before_stop async and tidy up async handlers removing Box::pin

    let wrapper = ContextWrapper { wrapped: Some(context) };

    let mut actor = System::new_actor(wrapper);
    actor.ctx.act_on_async::<Ping>(|actor, _event| {
        let context = actor.state.wrapped.clone();
        Box::pin(async move {
            if let Some(mut context) = context {
                for _ in 0..20 {
                    context.pool_emit::<DistributionStrategy>("pool", Pong).await.expect("Failed to send Pong");
                }
                context.terminate().await.expect("nope");
            }
        })
    });
    let context = actor.spawn().await;
    context.emit(Ping).await?;
    context.terminate().await?;

    Ok(())
}

#[quasar_message]
pub struct Pong;


#[quasar_message]
pub struct Ping;


#[quasar_message]
pub enum FunnyJoke {
    ChickenCrossesRoad,
    Pun,
}

#[quasar_message]
pub enum AudienceReaction {
    Chuckle,
    Groan,
}

#[quasar_message]
pub enum Tally {
    AddCount,
}