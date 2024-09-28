use acton::prelude::*;

#[derive(Default, Debug)]
struct BasicActor {
    counter: i8,
}

#[derive(Clone, Debug)]
struct IncrementByMsg(i8);


#[tokio::main]
async fn main() {
    // launch the system (required)
    let mut system = ActonSystem::launch();

    // create an actor
    // the actor is not started until the start method is called
    let mut actor = system.create_actor::<BasicActor>().await;

    // configure the actor
    // all the actor lifecycle events are optional but an actor without any event handlers has little (but not zero) utility
    actor
        .on_starting(|_actor| {
            println!("starting actor");
            ActorRef::noop()
        })
        .on_started(|_actor| {
            println!("started actor");
            ActorRef::noop()
        })
        .act_on::<IncrementByMsg>(|actor, event| {
            println!("incrementing by ({})", event.message.0);
            actor.entity.counter += event.message.0;
            ActorRef::noop()
        })
        .on_before_stop(|_actor| {
            println!("stopping actor");
            ActorRef::noop()
        })
        .on_stopped(|actor| {
            println!("stopped with total ({})", actor.entity.counter);
            ActorRef::noop()
        });

    let context = actor.start().await;

    //sending messages
    context.emit(IncrementByMsg(1)).await;
    context.emit(IncrementByMsg(10)).await;
    context.emit(IncrementByMsg(-8)).await;

    //shut down the system and all actors waiting for completion of all actors
    system.shutdown().await.expect("Failed to shutdown system");
}
