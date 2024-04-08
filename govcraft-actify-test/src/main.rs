use govcraft_actify::govcraft_actor;
use govcraft_actify_core::prelude::*;
// use govcraft_actify_core::govcraft_main;

#[govcraft_actor]
struct MyActor {
    name: String,
}

#[govcraft_async]
impl GovcraftActor for MyActor {
    type T = ActorMessage;

    async fn handle_message(&mut self, message: Self::T) {
        match message {
            ActorMessage::NewRecord(_record) => {
                // Add logic to process a new record here.
                println!("new record received for {}", self.name);
            }
            ActorMessage::ProcessingComplete(_barrier) => {
                println!("processing complete received");
            }
            _ => {}
        }
    }
}

#[tokio::main]
async fn main() {
    let (sender, _) = tokio::sync::broadcast::channel(512); // Adjust capacity as needed
    let name = "Dummy Processor Actor".to_string();
    let _ = MyActorContext::new(sender.subscribe(), name.clone());
    let _ = sender.send(ActorMessage::NewRecord(name));
}

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}