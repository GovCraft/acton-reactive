use crate::traits::message::GovcraftMessage;

// use govcraft_actify::traits::message::GovcraftMessage;
#[derive(Debug, Clone)]
pub enum SupervisorMessage{
    Shutdown,
    Terminate
}

impl GovcraftMessage for SupervisorMessage {

}
unsafe impl Send for SupervisorMessage {

}