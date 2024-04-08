mod actors;

use govcraft_actify::govcraft_actor;
use govcraft_actify_core::prelude::*;
use crate::actors::MyActorContext;
// use govcraft_actify_core::govcraft_main;

#[derive(Clone)]
pub struct MyMsg(String);


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