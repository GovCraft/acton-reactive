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

    let mut comedy_show = akton.create::<Comedian>(); //::<Comedian>::create_with_config(actor_config);

    // let actor_config = ActorConfig::new(
    //     "counter",
    //     None,
    //     Some(broker.clone()),
    // );
    // let mut counter_actor = akton.create::<Counter>();
    // counter_actor
    //     .setup
    //     .act_on::<Pong>(|actor, event| {
    //         info!("Also SUCCESS! PONG!");
    //     });

    comedy_show
        .setup
        .act_on_async::<Ping>(|actor, event| {
            info!("SUCCESS! PING!");
            Box::pin(async move {})
        })
        .act_on::<Pong>(|actor, event| {
            info!("SUCCESS! PONG!");
        });

    // counter_actor.context.subscribe::<Pong>().await;
    comedy_show.context.subscribe::<Ping>().await;
    comedy_show.context.subscribe::<Pong>().await;

    let comedian = comedy_show.activate(None);
    // let counter = counter_actor.activate(None);

    broker.emit_async(BrokerRequest::new(Ping), None).await;
    broker.emit_async(BrokerRequest::new(Pong), None).await;

    let _ = comedian.suspend().await?;
    // let _ = counter.suspend().await?;
    let _ = broker.suspend().await?;
    Ok(())
}
