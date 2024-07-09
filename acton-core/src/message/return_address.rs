use acton_ern::prelude::*;
use derive_new::new;

use crate::common::Outbox;

#[derive(new, Clone, Debug)]
pub struct ReturnAddress {
    pub address: Outbox,
    pub sender: Ern<UnixTime>,
}

impl Default for ReturnAddress {
    fn default() -> Self {
        let (outbox, _) = tokio::sync::mpsc::channel(1);
        Self::new(outbox, Ern::default())
    }
}