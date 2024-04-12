use crate::traits::message::GovcraftMessage;

#[derive(Debug, Clone)]
pub enum SupervisorMessage{
    Start
}

impl GovcraftMessage for SupervisorMessage {

}
unsafe impl Send for SupervisorMessage {

}