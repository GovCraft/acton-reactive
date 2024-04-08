use govcraft_actify::govcraft_actor;
use govcraft_actify_core::prelude::*;
// use govcraft_actify_core::govcraft_main;

#[derive(Clone)]
struct MyMsg(String);

#[govcraft_actor("self::MyMsg")]
struct MyActor {
    name: String,
}

#[govcraft_async]
impl GovcraftActor for MyActor {
    type T = MyMsg;

    async fn handle_message(&mut self, message: Self::T) {
        println!("messaged {} for actor with name {}", message.0, self.name);
    }
}

#[tokio::main]
async fn main() {
    let (sender, _) = broadcast::channel(512); // Adjust capacity as needed
    let name = "Dummy Processor Actor".to_string();
    let _ = MyActorContext::new(sender.subscribe(), name.clone());
    let _ = sender.send(MyMsg("hello".to_string()));
}

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}