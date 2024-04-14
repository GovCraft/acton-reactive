#[cfg(feature = "supervisor")]
mod supervision;
mod govcraft_system;
pub mod prelude {
    pub use govcraft_actify_core::prelude::*;
    pub use govcraft_actify_macro::{govcraft_actor, actify_message};
    pub use crate::govcraft_system::GovcraftSystem;
    // pub use crate::traits::{actor, message};
}
