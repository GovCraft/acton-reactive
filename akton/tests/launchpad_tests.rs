use std::any::TypeId;
use akton_test::prelude::*;
use tokio::runtime::Runtime;
use tokio::task;
use tracing::*;
use tracing::field::debug;

use akton::prelude::*;
use akton::prelude::Subscriber;

use crate::setup::*;

mod setup;

use tokio;
use std::time::Duration;

#[akton_test]
async fn test_launch_passing_akton() -> anyhow::Result<()> {
    initialize_tracing();
    let mut akton_ready: AktonReady = Akton::launch().into();
    let broker = akton_ready.get_broker();

    let actor_config = ActorConfig::new(
        Arn::with_root("parent")?,
        None,
        Some(broker.clone()),
    )?;

    let broker_clone = broker.clone();
    let parent_actor = akton_ready.clone().spawn_actor_with_setup::<Parent>(actor_config, |mut actor| Box::pin(async move {
        let child_actor_config = ActorConfig::new(
            Arn::with_root("child").expect("Could not create child ARN root"),
            None,
            Some(broker.clone()),
        ).expect("Couldn't create child config");

        let mut akton = akton_ready.clone();

        let child_context = akton.spawn_actor_with_setup::<Parent>(child_actor_config, |mut child| Box::pin(async move {
            child.setup
                .act_on::<Pong>(|_actor, _msg| {
                    info!("CHILD SUCCESS! PONG!");
                });

            let child_context = &child.context.clone();
            child_context.subscribe::<Pong>().await;
            child.activate(None).await
        })).await.expect("Couldn't create child actor");

        actor.setup
            .act_on::<Ping>(|_actor, _msg| {
                info!("SUCCESS! PING!");
            })
            .act_on_async::<Pong>(|actor, _msg| {
                ActorRef::wrap_future(wait_and_respond())
            });
        let context = &actor.context.clone();

        context.subscribe::<Ping>().await;
        context.subscribe::<Pong>().await;

        actor.activate(None).await
    })).await?;

    broker_clone.emit(BrokerRequest::new(Ping), None).await;
    broker_clone.emit(BrokerRequest::new(Pong), None).await;

    parent_actor.suspend().await?;
    broker_clone.suspend().await?;
    Ok(())
}

async fn wait_and_respond(){
    tokio::time::sleep(Duration::from_secs(1)).await;
    info!("Waited, then...SUCCESS! PONG!");
}

#[akton_test]
async fn test_launchpad() -> anyhow::Result<()> {
    initialize_tracing();
    let mut akton_ready: AktonReady = Akton::launch().into();

    let broker = akton_ready.get_broker();

    let actor_config = ActorConfig::new(
        Arn::with_root("improve_show")?,
        None,
        Some(broker.clone()),
    )?;

    let comedian_actor = akton_ready.spawn_actor::<Comedian>(|mut actor| Box::pin(async move {
        actor.setup
            .act_on::<Ping>(|_actor, _msg| {
                info!("SUCCESS! PING!");
            })
            .act_on_async::<Pong>(|_actor, _msg| {
                Box::pin(async move {
                    info!("SUCCESS! PONG!");
                })
            });

        actor.context.subscribe::<Ping>().await;
        actor.context.subscribe::<Pong>().await;

        actor.activate(None).await
    })).await?;

    let counter_actor = akton_ready.spawn_actor::<Counter>(|mut actor| Box::pin(async move {
        actor.setup
            .act_on_async::<Pong>(|_actor, _msg| {
                Box::pin(async move {
                    info!("SUCCESS! PONG!");
                })
            });

        actor.context.subscribe::<Pong>().await;

        actor.activate(None).await
    })).await?;

    broker.emit(BrokerRequest::new(Ping), None).await;
    broker.emit(BrokerRequest::new(Pong), None).await;

    broker.suspend().await?;
    comedian_actor.suspend().await?;
    counter_actor.suspend().await?;
    Ok(())
}