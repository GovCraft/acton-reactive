mod actors;

use tokio::signal;
use govcraft_actify::prelude::*;
use crate::actors::{DebugActorContext, MyActorContext};
// use govcraft_actify_core::govcraft_main;

#[derive(Clone, Debug)]
pub enum MyMsg
{
    Message(String),
    Whisper(String),
}


#[tokio::main]
async fn main() -> Result<()> {
    let (sender, _) = broadcast::channel(512); // Adjust capacity as needed
    let name = "MyActor".to_string();
    let _ = MyActorContext::new(sender.clone(), sender.subscribe(), name.clone());
    let _ = DebugActorContext::new(sender.clone(), sender.subscribe(), "Debug".to_string());
    let _ = sender.send(MyMsg::Message("hello".to_string()))?;

    match signal::ctrl_c().await {
        Ok(()) => {},
        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
        },
    }
    Ok(())
}

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}