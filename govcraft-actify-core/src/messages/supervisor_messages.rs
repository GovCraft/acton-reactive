use crate::traits::message::GovcraftMessage;

// use govcraft_actify::traits::message::GovcraftMessage;
#[derive(Debug, Clone)]
pub enum SupervisorMessage{
    Start,
    Shutdown
}

impl GovcraftMessage for SupervisorMessage {

}
unsafe impl Send for SupervisorMessage {

}