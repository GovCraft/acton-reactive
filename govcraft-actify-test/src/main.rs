use govcraft_actify::govcraft_actor;
use govcraft_actify_core::prelude::*;

pub struct MyMessage(String);
#[govcraft_actor]
struct MyActor {
    name: &'a str,
}


fn main() {
    // let actor = MyActor{name: String::from("me"), me: "" };
    println!("another Actor created!");
}

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}