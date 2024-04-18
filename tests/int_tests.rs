use std::any::Any;
use std::sync::{Arc, Mutex};
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
    mutation_count:usize
}

#[tokio::test]
async fn test_actor_mutation() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .compact()
        .with_line_number(true)
        .without_time()
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let my_state = MyCustomState {
        data: "Initial State".to_string(),
        mutation_count:0,
    };

    //creates a new root actor (system singularity) and begins processing system messages
    let system = QuasarSystem::new().await;

    let mut dormant_actor = system.singularity.new_quasar::<MyCustomState>(my_state, "my_state");
    assert_eq!(dormant_actor.ctx.qrn.value, "qrn:quasar:system:framework:root/my_state");

    let final_state = Arc::new(Mutex::new(String::new()));
    let final_state_clone = final_state.clone();  // Clone for use in the closure

    dormant_actor.ctx.on_before_start(|actor| {
        trace!("before starting actor");
    })
        .act_on::<FunnyMessage>(move |actor, msg|
            {
                warn!("MUTATING: actor was {}",actor.state.data);
                match msg {
                    FunnyMessage::Haha => {
                        actor.state.data = "Haha".to_string();
                    }
                    FunnyMessage::Lol => {
                        actor.state.data = "Lol".to_string();
                    }
                }
                info!("actor now {}",actor.state.data.clone());
                let mut state_lock = final_state_clone.lock().unwrap();
                *state_lock = actor.state.data.clone();
            });

    let mut context = Quasar::spawn(dormant_actor).await;

    context.send(FunnyMessage::Lol).await?;
    context.send(FunnyMessage::Haha).await?;
    //
    //
    // let _ = context.stop().await;
    let _ = system.singularity.stop().await;

    let final_result = final_state.lock().unwrap(); // Lock to access data safely
    assert_eq!(*final_result, "Haha");

    Ok(())
}

#[tokio::test]
async fn test_multiple_actor_mutation() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .compact()
        .with_line_number(true)
        .without_time()
        .finish();

    // tracing::subscriber::set_global_default(subscriber)
    //     .expect("setting default subscriber failed");

    let my_state = MyCustomState {
        data: "Initial State".to_string(),
        mutation_count:0,

    };
    let second_my_state = MyCustomState {
        data: "I'm number two!".to_string(),
        mutation_count:0,

    };

    //creates a new root actor (system singularity) and begins processing system messages
    let system = QuasarSystem::new().await;

    let mut dormant_actor = system.singularity.new_quasar::<MyCustomState>(my_state, "my_state");
    let mut second_dormant_actor = system.singularity.new_quasar::<MyCustomState>(second_my_state, "second_my_state");
    assert_eq!(dormant_actor.ctx.qrn.value, "qrn:quasar:system:framework:root/my_state");
    assert_eq!(second_dormant_actor.ctx.qrn.value, "qrn:quasar:system:framework:root/second_my_state");

    let final_state = Arc::new(Mutex::new(String::new()));
    let final_state_clone = final_state.clone();  // Clone for use in the closure


    let second_final_state = Arc::new(Mutex::new(String::new()));
    let second_final_state_clone = second_final_state.clone();  // Clone for use in the closure

    dormant_actor.ctx.on_before_start(|actor| {
        trace!("before starting actor");
    })
        .act_on::<FunnyMessage>(move |actor, msg|
            {
                warn!("MUTATING: actor was {}",actor.state.data);
                debug!("Actor {}",actor.qrn.value);
                match msg {
                    FunnyMessage::Haha => {
                        actor.state.data = "Haha".to_string();
                    }
                    FunnyMessage::Lol => {
                        actor.state.data = "Lol".to_string();
                    }
                }
                info!("actor now {}",actor.state.data.clone());
                actor.state.mutation_count +=1 ;
                let mut state_lock = final_state_clone.lock().unwrap();
                *state_lock = actor.state.data.clone();
                info!("Actor mutation count {}", actor.state.mutation_count);
            });

    second_dormant_actor.ctx.on_stop(|actor| {
        info!("after stopping actor");
    })
        .act_on::<Message>(move |actor, msg|
            {
                warn!("MUTATING: actor was {}",actor.state.data);
                debug!("Actor {}",actor.qrn.value);
                match msg {
                    Message::Hello => {
                        actor.state.data = "Hello".to_string();
                    }
                    Message::Hola => {
                        actor.state.data = "Hola".to_string();
                    }
                }
                info!("actor now {}",actor.state.data.clone());
                actor.state.mutation_count +=1 ;
                let mut state_lock = second_final_state_clone.lock().unwrap();
                *state_lock = actor.state.data.clone();

                info!("Actor mutation count {}", actor.state.mutation_count);
            });

    let mut context = Quasar::spawn(dormant_actor).await;
    let mut second_context = Quasar::spawn(second_dormant_actor).await;

    context.send(FunnyMessage::Lol).await?;
    second_context.send(Message::Hello).await?;
    context.send(FunnyMessage::Haha).await?;
    second_context.send(Message::Hola).await?;

    let _ = system.singularity.stop().await;

    let final_result = final_state.lock().unwrap(); // Lock to access data safely
    assert_eq!(*final_result, "Haha");

    let second_final_result = second_final_state.lock().unwrap(); // Lock to access data safely
    assert_eq!(*second_final_result, "Hola");

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