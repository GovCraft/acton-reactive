
pub use tokio::main as govcraft_main;
pub use tokio::runtime::Builder;
pub use tokio::*;

mod message_tracking;
mod context;
mod messages;
mod traits;
mod common;

pub mod prelude {
    // Re-exporting Tokio types
    pub use tokio::sync::{broadcast, mpsc, mpsc::channel, Notify};
    pub use tokio::{select, spawn};
    pub use async_trait::async_trait as govcraft_async;
    pub use anyhow::Result;
    pub use std::sync::{Arc, Mutex};
    pub use tokio::task::JoinHandle;
    pub use crate::messages::{SupervisorMessage, SystemMessage};
    pub use crate::traits::actor::GovcraftActor;
}

// #[non_exhaustive]
// #[derive(Clone, Debug)]
// pub enum ActorMessage {
//     /// Indicates a new record is ready to be processed.
//     NewRecord(String),
//     CountError,
//     ProcessingComplete(Arc<Barrier>),
// }
//
// #[non_exhaustive]
// #[derive(Clone, Debug)]
// pub enum ActorSupervisorMessage {
//     Shutdown
// }
