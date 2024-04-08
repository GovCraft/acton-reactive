use std::sync::Arc;
use tokio::sync::Barrier;
use async_trait::async_trait as govcraft_async;
use anyhow::Result;
pub use tokio::main as govcraft_main;
pub use tokio::runtime::Builder;
pub use tokio::*;

pub mod prelude {
    // Re-exporting Tokio types
    pub use tokio::sync::{broadcast, mpsc, mpsc::channel};
    pub use tokio::{spawn, select};
    pub use super::ActorMessage;
    pub use super::GovcraftActor;
    // pub use std::thread::Builder;
    pub use async_trait::async_trait as govcraft_async;
    // If you have custom types or traits that are frequently used,
    // you should re-export them here as well.
    // pub use crate::your_module::{YourType, YourTrait};
    pub use anyhow::Result;
}

#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum ActorMessage {
    /// Indicates a new record is ready to be processed.
    NewRecord(String),
    CountError,
    ProcessingComplete(Arc<Barrier>),
}

#[govcraft_async]
pub trait GovcraftActor {
    type T: Send + 'static;
    async fn handle_message(&mut self, message: Self::T) -> anyhow::Result<()>;
    async fn pre_run(&mut self)  -> anyhow::Result<()> { Ok(()) }
}