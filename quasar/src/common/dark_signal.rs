use std::any::Any;
use crate::traits::SingularitySignal;

#[derive(Debug)]
pub enum DarkSignal {
    Stop
}

impl SingularitySignal for DarkSignal {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
