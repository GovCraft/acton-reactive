use acton_ern::prelude::*;
use crate::common::Outbox;
use crate::message::Envelope;
use derive_new::new;
#[derive(new, Clone, Debug)]
pub struct ReturnAddress {
    pub address: Outbox,
    pub sender: Ern<UnixTime>,
}

impl Default for ReturnAddress {
    fn default() -> Self {
        todo!()
    }
}