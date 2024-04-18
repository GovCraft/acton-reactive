use std::any::Any;
use std::thread::sleep;
use std::time::Duration;
use tracing::{debug, info, Level, trace, warn};
use tracing_subscriber::FmtSubscriber;

use quasar::prelude::*;


// #[tokio::test]
// async fn test_on_stop() -> anyhow::Result<()> {
//     let mut actor = MyActor::new("actor".to_string(), "rrrodzilla".to_string());
//
//     actor.ctx
//         .on_stop(|actor| {
//             assert_eq!(actor.id, "actor");
//         });
//
//     let mut context = MyActor::spawn(actor).await;
//
//     // sleep(Duration::from_secs(2));
//
//     let _ = context.stop().await;
//
//
//     Ok(())
// }

#[tokio::test]
async fn test_on_start() -> anyhow::Result<()> {
    // let mut actor = Quasar::new("quasar", "system", "govcraft", "parser");
    // const PING_COUNT: usize = 2;
    // actor.ctx
    //     .on_start(|actor| {
    //         println!("now starting actor id: {}", actor.id);
    //         assert_eq!(actor.id, "root")
    //     })
    //     .on_before_start(|actor| {
    //         println!("before starting actor id: {}", actor.id);
    //         assert_eq!(actor.id, "root")
    //     })
    //     .on_stop(|actor| {
    //         println!("STOPPED actor id: {}", actor.id);
    //         assert_eq!(actor.id, "root");
    //     });
    //
    //
    // let mut ctx1 = Quasar::spawn(actor).await;
    //
    // // sleep(Duration::from_secs(2));
    // let _ = ctx1.stop().await;
    //

    Ok(())
}
#[derive(Default, Debug)]
pub struct MyCustomState {
    data: String,
}
#[tokio::test]
async fn test_singularity_qrn() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .compact()
        .with_line_number(true)
        .without_time()
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let my_state = MyCustomState {
        data: "Initial State".to_string(),
    };

    //creates a new root actor (system singularity) and begins processing system messages
    let system = QuasarSystem::new().await;
    //system singularity is type QuasarContext

    //a QuasarDormant struct with my_state attached is created and added as a child of the
    //root actor QuasarRunning<Singularity> (internally defined)

    let mut dormant_actor = system.singularity.new_quasar::<MyCustomState>(my_state, "my_state");
    assert_eq!(dormant_actor.ctx.qrn.value, "qrn:quasar:system:framework:root/my_state");
    //
    // //users pass closures for message processing specifying the type of message the closure handles
    dormant_actor.ctx.on_before_start(|actor|{
        trace!("before starting actor");
    })
        // dormant_actor.ctx
        .act_on::<FunnyMessage>(|actor, msg|
            {
                info!("funny message received");
                eprintln!("funny message received");
                // assert_eq!("Initial States", actor.state.data);
            });
    //     .act_on::<Ping>(|_, msg| {
    //         println!("Ping received.");
    //     });
    // // //consumes dormant_actor, uses into() to turn it into a QuasarRunning instance with a state field of some type to
    // // // hold the instance of MyCustomState,
    // // //spawns a Tokio task with the processing loop which consumes the QuasarRunning instance
    // // //and returns a QuasarContext for supervising the actor
    //
    let mut context = Quasar::spawn(dormant_actor).await;

    context.send(FunnyMessage::Lol).await?;

    //
    //
    // let _ = context.stop().await;
    let _ = system.singularity.stop().await;

    Ok(())
}

#[tokio::test]
async fn test_on_before_start() -> anyhow::Result<()> {
    // let system = QuasarSystem::new().await;

    // system.singularity.
    // let mut actor = Quasar::new("quasar", "system", "govcraft", "tester");
    //
    // actor.ctx
    //     .on_before_start(|actor| {
    //         assert_eq!(actor.id, "root");
    //     });
    //
    // let mut context = Quasar::spawn(actor).await;
    // let _ = context.stop().await;
    //


        Ok(())
}

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