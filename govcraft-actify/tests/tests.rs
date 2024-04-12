use std::thread::sleep;
use std::time::Duration;
use tokio::signal;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use tracing_subscriber::util::SubscriberInitExt;
use govcraft_actify::prelude::*;
use govcraft_actify::prelude::message::GovcraftMessage;

#[derive(Clone, Debug)]
pub enum MyMessage {
    Hello
}

impl GovcraftMessage for MyMessage {}

#[derive(Clone, Debug)]
pub enum BroadcastMessage {
    Hello
}

impl GovcraftMessage for crate::BroadcastMessage {}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_add_actor() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
// all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
// will be written to stdout.
        .with_max_level(Level::TRACE)
// completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let mut system = GovcraftSystem::new().await?;
    system.init().await?;
    assert!(true);
    // match signal::ctrl_c().await {
    //     Ok(()) => {},
    //     Err(err) => {
    //         eprintln!("Unable to listen for shutdown signal: {}", err);
    //     },
    // }

    Ok(())
}