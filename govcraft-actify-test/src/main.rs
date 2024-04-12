mod actors;

// use std::time::Duration;
// use tokio::signal;
use tracing::Level;
use tracing::trace;
use tracing_subscriber;
use tracing_subscriber::FmtSubscriber;
use govcraft_actify::prelude::*;
use govcraft_actify::prelude::SystemMessage;
// use govcraft_actify_macro::govcraft_actor;
// use govcraft_actify_core::prelude::*;
// use crate::actors::{DebugActorContext, MyActorContext};
// use govcraft_actify_core::govcraft_main;

#[derive(Clone, Debug)]
pub enum MyMsg
{
    Message(String),
    Whisper(String),
}


#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let mut system = GovcraftSystem::new().await?;
    system.init().await?;

    system.await_shutdown().await?;

    Ok(())
}
// match signal::ctrl_c().await {
//     Ok(()) => {},
//     Err(err) => {
//         eprintln!("Unable to listen for shutdown signal: {}", err);
//     },
// }
