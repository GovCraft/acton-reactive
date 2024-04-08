use std::sync::Arc;
use tokio::sync::Barrier;

pub mod prelude {
    // Re-exporting Tokio types
    pub use tokio::sync::{broadcast, mpsc, mpsc::Receiver, mpsc::Sender};
    pub use super::ActorMessage;
    // If you have custom types or traits that are frequently used,
    // you should re-export them here as well.
    // pub use crate::your_module::{YourType, YourTrait};
}

#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum ActorMessage {
    /// Indicates a new record is ready to be processed.
    NewRecord(String),
    CountError,
    ProcessingComplete(Arc<Barrier>),
}