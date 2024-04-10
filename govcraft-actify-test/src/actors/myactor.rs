use govcraft_actify::prelude::*;
use crate::MyMsg;

#[govcraft_actor("self::MyMsg")]
struct MyActor {
    name: String,
}

#[govcraft_async]
impl GovcraftActor for MyActor {
    type T = MyMsg;

    async fn handle_message(&mut self, message: Self::T, remaining: usize)-> anyhow::Result<()>{
        match message {
            MyMsg::Message(msg) => {
                println!("messaged {} for actor with name {}, now whispering...", msg, self.name);
                if let Some(internal) = &self.__internal {
                    let ctx = internal.context.lock().unwrap();
                    let _ = ctx.broadcast_sender.send(MyMsg::Whisper("pssst...hey".to_string()))?;
                }
                
            }
            _ => {}
        }
        Ok(())
    }
    async fn pre_run(&mut self) -> anyhow::Result<()> {
        if let Some(internal) = &self.__internal {
            let ctx = internal.context.lock().unwrap();
            let count= ctx.actors.len();
            println!("{} context has {} handles", self.name, count);
        }
        Ok(())
    }
}
