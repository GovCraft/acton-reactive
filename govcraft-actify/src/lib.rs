#[cfg(feature = "supervisor")]
mod supervision;
mod common;
mod govcraft_system;
pub mod prelude {
    pub use govcraft_actify_core::prelude::*;
    pub use govcraft_actify_macro::govcraft_actor;
    pub use crate::govcraft_system::GovcraftSystem;
    // pub use crate::traits::{actor, message};
}
