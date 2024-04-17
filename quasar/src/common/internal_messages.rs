use std::any::Any;
use crate::traits::LifecycleMessage;

#[derive(Debug)]
pub enum InternalMessage {
    Stop
}

impl LifecycleMessage for InternalMessage {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
