use akton_arn::prelude::*;

#[derive(Clone, Debug, Default)]
pub struct ReturnAddress {
    pub address: u64,
    pub sender: Arn,
}