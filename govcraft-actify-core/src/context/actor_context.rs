use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::sync::broadcast::{Sender, Receiver};
use tokio::task::JoinHandle;

pub struct ActorContext<SupervisorMessageType, BroadcastMessageType> {
    pub supervisor_sender: mpsc::Sender<SupervisorMessageType>,
    pub broadcast_sender: Sender<BroadcastMessageType>,
    pub actors: Vec<JoinHandle<()>>,
}

// impl<SupervisorMessageType, BroadcastMessageType> ActorContext<SupervisorMessageType, BroadcastMessageType> {
//     pub fn new<SupervisorMessageType, BroadcastMessageType>(broadcast_sender: Sender<BroadcastMessageType>, broadcast_receiver: Receiver<BroadcastMessageType>) -> Arc<Mutex<Self>> {
//         let (supervisor_sender, supervisor_receiver) = tokio::sync::mpsc::channel(255);
//         let context = Arc::new(Mutex::new(Self {
//             supervisor_sender,
//             broadcast_sender,
//             actors: vec![],
//         }));
//
//         let actor_context = Arc::clone(&context);
//         // let handle = tokio::spawn( async move {
//         //     let mut actor = #name ::new(Some(#internal_name {supervisor_receiver, broadcast_receiver, context:actor_context}), #new_args_defaults );
//         //     actor.pre_run().await.expect("Could not execute actor pre_run");
//         //     actor.run().await.expect("Could not execute actor run");
//         // });
//         {
//             let mut ctx = context.lock().unwrap();
//             // ctx.actors.push(handle);
//         }
//
//         context
//     }
//
// }