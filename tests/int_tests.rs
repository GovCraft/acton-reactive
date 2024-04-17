use std::any::Any;
use std::thread::sleep;
use std::time::Duration;

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

#[tokio::test]
async fn test_singularity_qrn() -> anyhow::Result<()> {
    let system = QuasarSystem::new().await;
    let c = system.singularity;
    assert_eq!(system.singularity.qrn().value, "qrn:quasar:system:framework:root");

    Ok(())
}

#[tokio::test]
async fn test_on_before_start() -> anyhow::Result<()> {
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