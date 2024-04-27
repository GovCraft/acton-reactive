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
use tracing::{debug, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Default, Debug)]
pub struct Counter {
    pub count: usize,
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

    let counter = Counter {
        count: 0,
    };

    let mut joke_counter = System::new(counter);

    // assert_eq!(system.context.key().value, "qrn:quasar:system:framework:root");

    //this is where S is specified
    // let mut joke_counter = system.state.context.new_actor::<Counter>(counter, "counter");
    // assert_eq!(joke_counter.state.key.value, "qrn:quasar:system:framework:root/counter");
    joke_counter.state.act_on_async::<FunnyMessage>(|actor, _record: &EventRecord<&FunnyMessage>| {
        let count = actor.state.state.count;
        debug!("entering handler");
        if let Some(envelope) = actor.new_envelope() {
            Box::pin(async move {
                envelope.reply(Nope::No).await.expect("TODO: panic message");

                debug!("Count: {}", count);
            }
            )
        } else {
            Box::pin(async {})
        }
    })
        .act_on::<Nope>(|_actor, _event| {
            debug!("got nope message");
        });
    // async fn process_message<'a>(actor: &'a mut Awake<Counter, Context>, record: &'a EventRecord<&'a FunnyMessage>) -> impl Future<Output = ()> +'a  {
    //     let fut = async move {
    //         debug!("Processing message: {:?}", record.message);
    //     };
    //     fut
    // }

    let mut comedian = joke_counter.spawn().await;

    // let mut comedian = Actor::spawn(joke_counter).await;
    //
    comedian.emit(FunnyMessage::Haha).await?;
    // let _ = comedian.terminate().await;
    // let _ = system.context.terminate().await;
    //
    Ok(())
}

#[quasar_message]
pub enum FunnyMessage {
    Haha,
    Lol,
    Giggle,
}

#[quasar_message]
pub enum Nope {
    No,
}
