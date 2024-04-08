use govcraft_actify::govcraft_actor;
use govcraft_actify_core::GovcraftActor;
use govcraft_actify_core::prelude::govcraft_async;
use crate::MyMsg;

#[govcraft_actor("self::MyMsg")]
struct MyActor {
    name: String,
}

#[govcraft_async]
impl GovcraftActor for MyActor {
    type T = MyMsg;

    async fn handle_message(&mut self, message: Self::T)-> anyhow::Result<()>{
        println!("messaged {} for actor with name {}", message.0, self.name);
        Ok(())
    }
    async fn pre_run(&mut self) -> anyhow::Result<()> {
        println!("from pre_run");
        Ok(())
    }
}
