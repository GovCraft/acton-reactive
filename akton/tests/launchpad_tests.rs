use std::any::TypeId;

use tokio::runtime::Runtime;
use tracing::*;

use akton::prelude::*;

use crate::setup::*;

mod setup;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_launchpad() -> anyhow::Result<()> {
    init_tracing();
    let mut akton: AktonReady = Akton::launch().into();

    let broker = akton.broker();

    let actor_config = ActorConfig::new(
        "improve_show",
        None,
        Some(broker.clone()),
    );

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

        actor.activate(None) // Return the configured actor
    })).await;

    let counter = akton.spawn_actor::<Counter>(|mut actor| Box::pin(async move {
        actor.setup
            .act_on::<Pong>(|actor, event| {
                info!("Also SUCCESS! PONG!");
            });

        // Subscribe to broker events
        actor.context.subscribe::<Pong>().await;

        actor.activate(None) // Return the configured actor
    })).await;

    broker.emit_async(BrokerRequest::new(Ping), None).await;
    broker.emit_async(BrokerRequest::new(Pong), None).await;

    let _ = comedian.suspend().await?;
    let _ = counter.suspend().await?;
    let _ = broker.suspend().await?;
    Ok(())
}
