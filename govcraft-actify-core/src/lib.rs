
use std::sync::Arc;
use tokio::sync::Barrier;
use async_trait::async_trait as govcraft_async;
pub use tokio::main as govcraft_main;
pub use tokio::runtime::Builder;
pub use tokio::*;

mod message_tracking;
mod context;

pub mod prelude {
    // Re-exporting Tokio types
    pub use tokio::sync::{broadcast, mpsc, mpsc::channel, Notify};
    pub use tokio::{spawn, select};
    pub use super::ActorMessage;
    pub use super::ActorSupervisorMessage;
    pub use super::GovcraftActor;
    // pub use std::thread::Builder;
    pub use async_trait::async_trait as govcraft_async;
    // If you have custom types or traits that are frequently used,
    // you should re-export them here as well.
    // pub use crate::your_module::{YourType, YourTrait};
    pub use anyhow::Result;
    pub use std::sync::{Arc, Mutex};
    pub use tokio::task::JoinHandle;
}

#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum ActorMessage {
    /// Indicates a new record is ready to be processed.
    NewRecord(String),
    CountError,
    ProcessingComplete(Arc<Barrier>),
}

#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum ActorSupervisorMessage {
    Shutdown
}

#[govcraft_async]
pub trait GovcraftActor {
    type T: Send + 'static;
    async fn handle_message(&mut self, message: Self::T) -> anyhow::Result<()>;
    async fn handle_supervisor_message(&mut self, _message: ActorSupervisorMessage) -> anyhow::Result<()>{
        Ok(())
    }
    async fn pre_run(&mut self)  -> anyhow::Result<()> { Ok(()) }
    async fn shutdown(&mut self)  -> anyhow::Result<()> { Ok(()) }
    async fn handle_message_internal(&mut self, message: Self::T) -> anyhow::Result<()> {
        println!("internal");
        self.handle_message(message).await?;
        Ok(())
    }
}

mod private {
    // This trait is private and acts as a seal.
    // Only types within the `actor` module can implement it.
    pub trait Sealed {}
}
