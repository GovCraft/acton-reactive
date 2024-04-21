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
// use std::sync::{Arc, Mutex};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Default, Debug)]
pub struct Counter {
    pub count: usize,
}

#[tokio::test]
async fn test_actor_mutation() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .compact()
        .with_line_number(true)
        .without_time()
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let counter = Counter {
        count: 0,
    };

    let system = System::spawn().await;
    assert_eq!(system.context.key().value, "qrn:quasar:system:framework:root");

    //this is where S is specified
    let mut joke_counter = system.context.new_actor::<Counter>(counter, "counter");
    assert_eq!(joke_counter.state.key.value, "qrn:quasar:system:framework:root/counter");

    joke_counter.state.act_on::<FunnyMessage>(|actor, event| {
        actor.state.count += 1;
        match event.message {
            FunnyMessage::Haha => {
                info!("I laughed");
            }
            FunnyMessage::Lol => {
                info!("I lol'ed");
            }
            FunnyMessage::Giggle => {
                info!("I giggled");
            }
        }
        info!("Jokes told: {}",actor.state.count);
        info!("{:?}",event.sent_time);
    })
        .on_stop(|actor| {
             info!("count: {}", actor.state.count);
            assert_eq!(actor.state.count, 3);
        });

    let mut comedian = Actor::spawn(joke_counter).await;

    comedian.emit(FunnyMessage::Haha).await?;
    comedian.emit(FunnyMessage::Giggle).await?;
    comedian.emit(FunnyMessage::Lol).await?;

    let _ = system.context.terminate().await;
    let _ = comedian.terminate().await;

    Ok(())
}

#[quasar_message]
pub enum FunnyMessage {
    Haha,
    Lol,
    Giggle,
}
