use std::any::{Any, TypeId};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use quasar::*;
use quasar::prelude::*;


//region Main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    const PING_COUNT: usize = 2;

    // let mut actor = Quasar::();
    //
    // actor.ctx
    //     .act_on::<FunnyMessage>(|actor, msg|
    //         {
    //             println!("{:?}", msg);
    //         });
    //
    // let mut ctx1 = Quasar::spawn(actor).await;
    // ctx1.send(FunnyMessage::Lol).await?;
    //
    //
    // // sleep(Duration::from_secs(2));
    // let _ = ctx1.stop().await;


    Ok(())
}
// .act_on::<FunnyMessage>(|actor, msg| {
//     println!("Handling FunnyMessage: {:?}", msg);
//     match msg {
//         FunnyMessage::Haha => {
//             println!("Haha received");
//             // actor.hello();
//         }
//         FunnyMessage::Lol => {
//             println!("Lol received");
//         }
//     }
// })
// .act_on::<Ping>(|_, msg| {
//     println!("Ping {}", msg.0);
//     if msg.0 == PING_COUNT - 1 {
//         println!("Last ping received, should not stop actor.");
//     }
// })
// .act_on::<Message>(|actor, msg| {
//     println!("Handling Message: {:?}", msg);
//
//     match msg {
//         Message::Hello => println!("Hello received"),
//         Message::Hola => println!("Hola received"),
//     }
// })

#[test]
fn test_url_builder( )
{
    use url_builder::URLBuilder;

    let mut ub = URLBuilder::new();

    ub.set_protocol("http")
        .set_host("localhost")
        .set_port(8000)
        .add_param("first", "1")
        .add_param("second", "2")
        .add_param("third", "3")
        .add_route("some id");

    println!("{}", ub.build());
}
#[tokio::test]
async fn test_on_start() -> anyhow::Result<()> {
    // let mut actor = Quasar::new("quasar", "system", "govcraft", "root");
    // const PING_COUNT: usize = 2;
    //
    // let count = Arc::new(Mutex::new(0));
    // actor.ctx
    //     .on_start(|actor| {
    //         println!("now starting actor id: {}", actor.id);
    //         assert!(true);
    //     })
    //     .act_on::<FunnyMessage>(move |actor, msg| {
    //         println!("{:?}", msg);
    //         let count = count.clone();
    //         let mut count = count.lock().unwrap() ;
    //         *count += 1;
    //         println!("count: {}", count);
    //     });
    //
    //
    // let mut ctx1 = Quasar::spawn(actor).await;
    //
    // ctx1.send(FunnyMessage::Lol).await?;
    // ctx1.send(FunnyMessage::Lol).await?;
    // ctx1.send(FunnyMessage::Lol).await?;
    // ctx1.send(FunnyMessage::Lol).await?;
    //
    //
    // for i in 0..PING_COUNT {
    //     ctx1.send(Ping(i)).await?; // Send a message to the selected context
    // }
    //
    //
    // let _ = ctx1.stop().await;

    Ok(())
}

//endregion
#[derive(Debug)]
pub enum Message {
    Hello,
    Hola,
}

impl ActorMessage for Message {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub enum DifferentMessage {
    Sup,
    Suuuup,
}

impl ActorMessage for DifferentMessage {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub enum FunnyMessage {
    Haha,
    Lol,
}

impl ActorMessage for FunnyMessage {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct Ping(usize);

impl ActorMessage for Ping {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

//region Test Message Types

//endregion
