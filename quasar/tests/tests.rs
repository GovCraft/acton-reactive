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

use quasar::prelude::async_trait::async_trait;
use quasar::prelude::*;
use std::sync::Once;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[derive(Default, Debug)]
pub struct Comedian {
    pub jokes_told: usize,
    pub funny: usize,
    pub bombers: usize,
}

#[tokio::test]
async fn test_actor_mutation() -> anyhow::Result<()> {
    init_tracing();

    let mut comedy_show = System::new_actor(Comedian::default());

    comedy_show
        .ctx
        .act_on_async::<FunnyJoke>(|actor, record: &EventRecord<&FunnyJoke>| {
            actor.state.jokes_told += 1;

            if let Some(envelope) = actor.new_envelope() {
                match record.message {
                    FunnyJoke::ChickenCrossesRoad => Box::pin(async move {
                        envelope
                            .reply(AudienceReaction::Chuckle, None)
                            .await
                            .expect("TODO: panic message");
                    }),
                    FunnyJoke::Pun => Box::pin(async move {
                        envelope
                            .reply(AudienceReaction::Groan, None)
                            .await
                            .expect("TODO: panic message");
                    }),
                }
            } else {
                Box::pin(async {})
            }
        })
        .act_on::<AudienceReaction>(|actor, event| match event.message {
            AudienceReaction::Chuckle => actor.state.funny += 1,
            AudienceReaction::Groan => actor.state.bombers += 1,
        })
        .on_stop(|actor| {
            tracing::debug!(
                "Jokes Told: {}\tFunny: {}\tBombers: {}",
                actor.state.jokes_told,
                actor.state.funny,
                actor.state.bombers
            );
        });

    let comedian = comedy_show.spawn().await;

    comedian.emit(FunnyJoke::ChickenCrossesRoad).await?;
    comedian.emit(FunnyJoke::Pun).await?;
    comedian.terminate().await;
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
    init_tracing();
    let counter = Counter { count: 0 };

    let mut count = System::new_actor(counter);
    count
        .ctx
        .act_on::<Tally>(|actor, _event| {
            tracing::warn!("on tally");
            actor.state.count += 1;
        })
        .on_stop(|actor| {
            assert_eq!(4, actor.state.count);
            tracing::warn!("on stopping");
        });
    let count = count.spawn().await;

    let actor = Messenger {
        sender: Some(count.return_address()),
    };
    let mut actor = System::new_actor(actor);
    actor
        .ctx
        .on_before_wake(|actor| {
            if let Some(envelope) = actor.state.sender.clone() {
                tokio::spawn(async move { envelope.reply(Tally::AddCount, None).await });
            }
        })
        .on_wake(|actor| {
            if let Some(envelope) = actor.state.sender.clone() {
                tokio::spawn(async move { envelope.reply(Tally::AddCount, None).await });
            }
        })
        .on_stop(|actor| {
            if let Some(envelope) = actor.state.sender.clone() {
                tokio::spawn(async move { envelope.reply(Tally::AddCount, None).await });
            }
        });

    let actor = actor.spawn().await;

    count.terminate().await;
    actor.terminate().await;
    Ok(())
}

#[quasar_message]
pub enum StatusReport {
    Complete(usize),
}

#[tokio::test]
async fn test_actor_pool_random() -> anyhow::Result<()> {
    init_tracing();
    let mut actor = System::new_actor(PoolItem::default());
    actor
        .ctx
        .act_on::<Pong>(|_actor, _event| {
            tracing::error!("PONG");
        })
        .act_on::<StatusReport>(|actor, event| {
            let sender = &event.return_address.as_ref().unwrap().sender;
            match event.message {
                StatusReport::Complete(total) => {
                    tracing::debug!("{} reported {}", sender.value, total);
                    actor.state.receive_count += total;
                }
            }
        })
        .on_before_stop(|actor| {
            tracing::warn!("Processed {} PONGs", actor.state.receive_count);
        });
    let builder = PoolBuilder::default().add_pool::<PoolItem>("pool", 5, LBStrategy::Random);
    let context = actor.spawn_with_pools(builder).await;

    for _ in 0..22 {
        tracing::trace!("Emitting PING");
        context.emit_pool("pool", Ping).await;
    }
    tracing::trace!("Terminating main actor");
    context.terminate().await;

    Ok(())
}

#[tokio::test]
async fn test_actor_pool_round_robin() -> anyhow::Result<()> {
    init_tracing();
    let mut actor = System::new_actor(PoolItem::default());
    actor
        .ctx
        .act_on::<Pong>(|_actor, _event| {
            tracing::error!("PONG");
        })
        .act_on::<StatusReport>(|actor, event| {
            let sender = &event.return_address.as_ref().unwrap().sender;

            match event.message {
                StatusReport::Complete(total) => {
                    tracing::debug!("{} reported {}", sender.value, total);
                    actor.state.receive_count += total;
                }
            }
        })
        .on_before_stop(|actor| {
            tracing::warn!("Processed {} PONGs", actor.state.receive_count);
        });
    let builder = PoolBuilder::default().add_pool::<PoolItem>("pool", 5, LBStrategy::RoundRobin);
    let context = actor.spawn_with_pools(builder).await;

    for _ in 0..22 {
        tracing::trace!("Emitting PING");
        context.emit_pool("pool", Ping).await;
    }
    tracing::trace!("Terminating main actor");
    context.terminate().await;

    Ok(())
}
#[derive(Default, Debug)]
pub struct ContextWrapper {
    pub wrapped: Option<Context>,
}
#[derive(Default, Debug)]
pub struct PoolItem {
    receive_count: usize,
}

#[async_trait]
impl ConfigurableActor for PoolItem {
    async fn init(&self, name: String, parent: &Context) -> Context {
        let mut actor = parent.new_actor::<PoolItem>(&name);
        actor
            .ctx
            .act_on::<Ping>(|actor, _event| {
                //tracing::debug!("{} PONG!", &actor.key.value);
                actor.state.receive_count += 1;
            })
            .on_before_stop_async(|actor| {
                let final_count = actor.state.receive_count;
                let value = actor.key.value.clone();

                if let Some(envelope) = actor.new_parent_envelope() {
                    Box::pin(async move {
                        tracing::trace!(
                            "Reporting {} complete to {} from {}.",
                            final_count,
                            envelope.sender,
                            value
                        );
                        let _ = envelope
                            .reply(StatusReport::Complete(final_count), None)
                            .await;
                    })
                } else {
                    Box::pin(async {})
                }
            });

        actor.spawn().await
    }
}
static INIT: Once = Once::new();

pub(crate) fn init_tracing() {
    INIT.call_once(|| {
        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::DEBUG)
            .compact()
            .with_line_number(true)
            .without_time()
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    });
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
