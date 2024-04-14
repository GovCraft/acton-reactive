mod actors;

use tokio::signal;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use tracing_subscriber::util::SubscriberInitExt;
use govcraft_actify::prelude::*;

#[actify_message]
pub enum MyMsg
{
    Message(String),
    Whisper(String),
}


#[tokio::main]
async fn main() -> Result<()> {
    let _ = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish().init();

    let system = GovcraftSystem::new().await?;

    // info!("Govcraft Actify running. Press CTRL-C to exit");
    // match signal::ctrl_c().await {
    //     Ok(()) => {
    //         system.await_shutdown().await?;
    //         info!("Govcraft actify shutdown success. Goodbye.")
    //     }
    //     Err(err) => {
    //         tracing::error!("Unable to listen for shutdown signal: {}", err);
    //     }
    // }
    Ok(())
}
