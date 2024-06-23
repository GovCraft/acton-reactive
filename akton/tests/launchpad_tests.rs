use std::any::TypeId;

use tokio::runtime::Runtime;
use tracing::*;
use tracing::field::debug;

use akton::prelude::*;
use akton::prelude::Subscriber;

use crate::setup::*;

mod setup;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_launch_passing_akton() -> anyhow::Result<()> {
    init_tracing();
    let mut akton: AktonReady = Akton::launch().into();
    let broker = akton.broker();

    let actor_config = ActorConfig::new(
        Arn::with_root("parent")?,
        None,
        Some(broker.clone()),
    )?;

    let parent = akton.spawn_actor_with_config::<Parent>(actor_config, |mut actor| Box::pin(async move {
        actor.setup
            .act_on::<Ping>(|actor, msg| {
                info!("SUCCESS! PING!");
            })
            .act_on_async::<Pong>(|actor, msg| {
                Box::pin(async move {
                    info!("SUCCESS! PONG!");
                })
            });
        let context = &actor.context.clone();

        context.subscribe::<Ping>().await;
        context.subscribe::<Pong>().await;

        actor.activate(None).await
    })).await?;


    broker.emit_async(BrokerRequest::new(Ping), None).await;
    broker.emit_async(BrokerRequest::new(Pong), None).await;

    let _ = parent.suspend().await?;

    let _ = broker.suspend().await?;
    Ok(())
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_launchpad() -> anyhow::Result<()> {
    init_tracing();
    let mut akton: AktonReady = Akton::launch().into();

    let broker = akton.broker();

    let actor_config = ActorConfig::new(
        Arn::with_root("improve_show")?,
        None,
        Some(broker.clone()),
    )?;

    // let mut comedy_show = akton.create::<Comedian>(); //::<Comedian>::create_with_config(actor_config);
    let comedian = akton.spawn_actor::<Comedian>(|mut actor| Box::pin(async move {
        actor.setup
            .act_on::<Ping>(|actor, msg| {
                info!("SUCCESS! PING!");
            })
            .act_on_async::<Pong>(|actor, msg| {
                Box::pin(async move {
                    info!("SUCCESS! PONG!");
                })
            });

        // Subscribe to broker events
        actor.context.subscribe::<Ping>().await;
        actor.context.subscribe::<Pong>().await;

        actor.activate(None).await // Return the configured actor
    })).await?;

    let counter = akton.spawn_actor::<Counter>(|mut actor| Box::pin(async move {
        actor.setup
            .act_on_async::<Pong>(|actor, msg| {
                Box::pin(async move {
                    info!("SUCCESS! PONG!");
                })
            });

        // Subscribe to broker events
        actor.context.subscribe::<Pong>().await;

        actor.activate(None).await // Return the configured actor
    })).await?;

    broker.emit_async(BrokerRequest::new(Ping), None).await;
    broker.emit_async(BrokerRequest::new(Pong), None).await;

    let _ = broker.suspend().await?;
    let _ = comedian.suspend().await?;
    let _ = counter.suspend().await?;
    Ok(())
}
