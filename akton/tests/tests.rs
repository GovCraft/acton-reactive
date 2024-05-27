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

use akton::prelude::async_trait::async_trait;
use akton::prelude::*;
use rand::Rng;
use std::sync::Once;
use tracing::Level;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::FmtSubscriber;

// A simple actor for testing purposes
// tracks the overall number of jokes told
// the number of jokes audience members found funny
// and the number of jokes which bombed with the audience
#[derive(Default, Debug, Clone)]
pub struct Comedian {
    pub jokes_told: usize,
    pub funny: usize,
    pub bombers: usize,
}

// We'll create pool of audience member actors who will hear a joke told by the comedian
// They will randomly react to the jokes after which the Comedian will report on how many
// jokes landed and didn't land

#[derive(Default, Debug, Clone)]
pub struct AudienceMember {
    pub jokes_told: usize,
    pub funny: usize,
    pub bombers: usize,
}

#[async_trait]
impl ConfigurableActor for AudienceMember {
    // this trait function details what should happen for each member of the pool we are about to
    // create, it gets created when the parent actor calls spawn_with_pool
    async fn init(&self, name: String, root: &Context) -> Context {
        let mut parent = root.new_actor::<AudienceMember>(&name);
        parent.ctx.act_on::<Joke>(|actor, _event| {
            let sender = &actor.new_parent_envelope();
            let mut rchoice = rand::thread_rng();
            let random_reaction = rchoice.gen_bool(0.5);
            if random_reaction {
                tracing::trace!("Send chuckle");
                let _ = sender.reply(AudienceReaction::Chuckle, Some("audience".to_string()));
            } else {
                tracing::trace!("Send groan");
                let _ = sender.reply(AudienceReaction::Groan, Some("audience".to_string()));
            }
        });
        let context = parent.spawn().await;
        context
    }
}
// the joke told by the comedian
#[akton_message]
pub struct Joke;

#[tokio::test(flavor = "multi_thread")]
async fn test_audience_pool() -> anyhow::Result<()> {
    init_tracing();
    let mut audience = System::<AudienceMember>::new_actor();

    audience
        .ctx
        .act_on::<AudienceReaction>(|actor, event| {
            match event.message {
                AudienceReaction::Groan => actor.state.funny += 1,
                AudienceReaction::Chuckle => actor.state.bombers += 1,
            }
            actor.state.jokes_told += 1;
        })
        .on_before_stop(|actor| {
            tracing::info!(
                "Jokes Told: {}\tFunny: {}\tBombers: {}",
                actor.state.jokes_told,
                actor.state.funny,
                actor.state.bombers
            );
        });
    let pool =
        PoolBuilder::default().add_pool::<AudienceMember>("audience", 1000, LBStrategy::Random);

    //here we call the init method on the 1000 pool members we created
    let context = audience.spawn_with_pools(pool).await;

    for _ in 0..1000 {
        context.emit_pool("audience", Joke).await;
    }
    //let result = context.peek_state::<AudienceMember>().await?;
    let result = context.terminate::<AudienceMember>().await?;
    assert_eq!(result.jokes_told, 1000);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_actor_mutation() -> anyhow::Result<()> {
    init_tracing();

    let mut comedy_show = System::<Comedian>::new_actor();

    comedy_show
        .ctx
        .act_on_async::<FunnyJoke>(|actor, record: &EventRecord<&FunnyJoke>| {
            actor.state.jokes_told += 1;

            if let Some(envelope) = actor.new_envelope() {
                match record.message {
                    FunnyJoke::ChickenCrossesRoad => Box::pin(async move {
                        let _ = envelope.reply(AudienceReaction::Chuckle, None);
                    }),
                    FunnyJoke::Pun => Box::pin(async move {
                        let _ = envelope.reply(AudienceReaction::Groan, None);
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
            tracing::info!(
                "Jokes Told: {}\tFunny: {}\tBombers: {}",
                actor.state.jokes_told,
                actor.state.funny,
                actor.state.bombers
            );
            assert_eq!(actor.state.jokes_told, 2);
        });

    let comedian = comedy_show.spawn().await;

    comedian.emit(FunnyJoke::ChickenCrossesRoad).await?;
    comedian.emit(FunnyJoke::Pun).await?;
    let _ = comedian.terminate::<Comedian>().await?;

    Ok(())
}

#[derive(Default, Debug, Clone)]
pub struct Counter {
    pub count: usize,
}

#[derive(Default, Debug, Clone)]
pub struct Messenger {
    pub sender: Option<OutboundEnvelope>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_lifecycle_handlers() -> anyhow::Result<()> {
    init_tracing();
    //    let counter = Counter { count: 0 };

    let mut count = System::<Counter>::new_actor();
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

    let mut actor = System::<Messenger>::new_actor();
    actor
        .ctx
        .on_before_wake(|actor| {
            if let Some(envelope) = actor.state.sender.clone() {
                tokio::spawn(async move { envelope.reply(Tally::AddCount, None) });
            }
        })
        .on_wake(|actor| {
            if let Some(envelope) = actor.state.sender.clone() {
                tokio::spawn(async move { envelope.reply(Tally::AddCount, None) });
            }
        })
        .on_stop(|actor| {
            if let Some(envelope) = actor.state.sender.clone() {
                tokio::spawn(async move { envelope.reply(Tally::AddCount, None) });
            }
        });

    let actor = actor.spawn().await;

    count.terminate::<Counter>().await?;
    actor.terminate::<Messenger>().await?;
    Ok(())
}

#[akton_message]
pub enum StatusReport {
    Complete(usize),
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_actor_pool_random() -> anyhow::Result<()> {
    init_tracing();
    let mut actor = System::<PoolItem>::new_actor();
    actor
        .ctx
        .act_on::<StatusReport>(|actor, event| {
            let sender = &event.return_address.sender;
            match event.message {
                StatusReport::Complete(total) => {
                    tracing::debug!("{} reported {}", sender.value, total);
                    actor.state.receive_count += total;
                }
            }
        })
        .on_before_stop(|actor| {
            tracing::info!("Processed {} PONGs", actor.state.receive_count);
        });
    let builder = PoolBuilder::default().add_pool::<PoolItem>("pool", 5, LBStrategy::Random);
    let context = actor.spawn_with_pools(builder).await;

    for _ in 0..22 {
        tracing::trace!("Emitting PING");
        context.emit_pool("pool", Ping).await;
    }
    tracing::trace!("Terminating main actor");
    context.terminate::<PoolItem>().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_actor_pool_round_robin() -> anyhow::Result<()> {
    init_tracing();
    let mut actor = System::<PoolItem>::new_actor();
    actor
        .ctx
        .act_on::<Pong>(|_actor, _event| {
            //            tracing::error!("PONG");
        })
        .act_on::<StatusReport>(|actor, event| {
            let sender = &event.return_address.sender;

            match event.message {
                StatusReport::Complete(total) => {
                    tracing::info!("{} reported {}", sender.value, total);
                    actor.state.receive_count += total;
                }
            }
        })
        .on_before_stop(|actor| {
            tracing::info!("Processed {} PONGs", actor.state.receive_count);
        });
    let builder = PoolBuilder::default().add_pool::<PoolItem>("pool", 5, LBStrategy::RoundRobin);
    let context = actor.spawn_with_pools(builder).await;

    for _ in 0..22 {
        //     tracing::trace!("Emitting PING");
        context.emit_pool("pool", Ping).await;
    }
    //    tracing::trace!("Terminating main actor");
    context.terminate::<PoolItem>().await?;

    Ok(())
}

#[derive(Default, Debug)]
pub struct ContextWrapper {
    pub wrapped: Option<Context>,
}
#[derive(Default, Debug, Clone)]
pub struct PoolItem {
    receive_count: usize,
}

#[async_trait]
impl ConfigurableActor for PoolItem {
    //    #[instrument(skip(self, name, parent))]
    async fn init(&self, name: String, parent: &Context) -> Context {
        //        tracing::trace!("Initializing actor with name: {}", name);

        let mut actor = parent.new_actor::<PoolItem>(&name);

        // Log the mailbox state immediately after actor creation
        tracing::trace!(
            "Actor initialized with key: {}, mailbox closed: {}",
            actor.key.value,
            actor.mailbox.is_closed()
        );

        actor
            .ctx
            .act_on::<Ping>(|actor, _event| {
                tracing::trace!(
                    "Received Ping event for actor with key: {}",
                    actor.key.value
                );
                actor.state.receive_count += 1;
            })
            .on_before_stop(|actor| {
                let final_count = actor.state.receive_count;

                let envelope = actor.new_parent_envelope();
                let to_address = envelope.sender.value.clone();
                let from_address = actor.key.value.clone();

                tracing::info!(
                    "Reporting {} complete to {} from {}.",
                    final_count,
                    to_address,
                    from_address,
                );
                let _ = envelope.reply(StatusReport::Complete(final_count), None);
            });

        //        tracing::trace!("Spawning actor with key: {}", &actor.key.value);

        let context = actor.spawn().await;

        //       tracing::trace!("Actor with key: {} spawned", &context.key.value);

        context
    }
}
static INIT: Once = Once::new();

pub(crate) fn init_tracing() {
    INIT.call_once(|| {
        // Define an environment filter to suppress logs from the specific function
        let filter = tracing_subscriber::EnvFilter::new("")
            .add_directive(
                "akton_core::common::context::peek_state_span=trace"
                    .parse()
                    .unwrap(),
            )
            .add_directive("akton_core::common::context=off".parse().unwrap())
            .add_directive("tests=off".parse().unwrap())
            .add_directive("akton_core::traits=off".parse().unwrap())
            .add_directive("akton_core::common::awake=off".parse().unwrap())
            .add_directive("akton_core::common::system=off".parse().unwrap())
            .add_directive("akton_core::common::supervisor=off".parse().unwrap())
            .add_directive("akton_core::common::actor=off".parse().unwrap())
            .add_directive("akton_core::common::idle=off".parse().unwrap())
            .add_directive(
                "akton_core::common::outbound_envelope=off"
                    .parse()
                    .unwrap(),
            )
            .add_directive(tracing_subscriber::filter::LevelFilter::TRACE.into()); // Set global log level to TRACE

        let subscriber = FmtSubscriber::builder()
            .with_span_events(FmtSpan::ENTER | FmtSpan::EXIT)
            .with_max_level(Level::TRACE)
            .compact()
            .with_line_number(true)
            .without_time()
            .with_env_filter(filter)
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    });
}

#[akton_message]
pub struct Pong;

#[akton_message]
pub struct Ping;

#[akton_message]
pub enum FunnyJoke {
    ChickenCrossesRoad,
    Pun,
}

#[akton_message]
pub enum AudienceReaction {
    Chuckle,
    Groan,
}

#[akton_message]
pub enum Tally {
    AddCount,
}
