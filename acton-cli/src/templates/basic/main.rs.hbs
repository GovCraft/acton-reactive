use acton_reactive::prelude::*;
use tracing_subscriber;
mod agents;
mod messages;

use agents::MyAgent;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
// Initialize tracing to log to a file
tracing_subscriber::fmt()
.with_max_level(tracing::Level::INFO)
.with_writer(std::fs::File::create("logs/app.log")?)
.init();

let mut app = ActonApp::launch();

// Example agent setup
let my_agent = app.new_agent::<MyAgent>().await;
    my_agent.start().await;

    // Shut down Acton system
    app.shutdown_all().await?;
    Ok(())
    }
