use acton_ern::prelude::*;
use crate::common::Outbox;
use crate::message::Envelope;

#[derive(Clone, Debug)]
pub struct ReturnAddress {
    pub address: Outbox,
    pub sender: Ern<UnixTime>,
}