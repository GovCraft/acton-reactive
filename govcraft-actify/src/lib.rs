#[cfg(feature = "supervisor")]
mod supervision;
mod common;
mod govcraft_system;
pub mod prelude {
    pub use govcraft_actify_core::prelude::*;
    pub use govcraft_actify_core::traits::actor::GovcraftActor;
    pub use govcraft_actify_macro::govcraft_actor;
    pub use govcraft_actify_core::messages::{SupervisorMessage, SystemMessage};
    pub use crate::govcraft_system::GovcraftSystem;
    // pub use crate::traits::{actor, message};
}
