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






use quasar_core::prelude::*;
use quasar::prelude::*;
use tracing::{debug, Level};
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

    let comedian = Comedian {
        jokes_told: 0,
        funny: 0,
        bombers: 0,
    };

    let mut comedy_show = System::new(comedian);

    comedy_show.actor_ref.act_on_async::<FunnyJoke>(|actor, record: &EventRecord<&FunnyJoke>| {
        actor.state.jokes_told += 1;

        if let Some(envelope) = actor.new_envelope() {
            match record.message {
                FunnyJoke::ChickenCrossesRoad => {
                    Box::pin(async move {
                        envelope.reply(AudienceReaction::Chuckle).await.expect("TODO: panic message");
                    }
                    )
                }
                FunnyJoke::Pun => {
                    Box::pin(async move {
                        envelope.reply(AudienceReaction::Groan).await.expect("TODO: panic message");
                    }
                    )
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
        .on_before_wake(|_actor| {
            debug!("on_before_wake");
        })
        .on_wake(|_actor| {
            debug!("on_wake");
        })
        .on_stop(|actor| {
            debug!("on_stop");
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
    // console_subscriber::init();

    let counter = Counter {
        count: 0,
    };

    let mut count = System::new(counter);
    count.actor_ref.act_on::<Tally>(|actor, _event| {
        actor.state.count += 1;
    }).on_stop(|actor| {
        assert_eq!(4, actor.state.count);
        debug_assert!(false);
    });
    let count = count.spawn().await;


    let actor = Messenger {
        sender: Some(count.return_address()),
    };
    let mut actor = System::new(actor);
    actor.actor_ref
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
    Ok(())
}


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