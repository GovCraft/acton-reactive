use crate::traits::message::GovcraftMessage;

#[derive(Clone,Debug)]
pub enum SupervisorMessage{
    Start
}

impl GovcraftMessage for SupervisorMessage {

}
unsafe impl Send for SupervisorMessage {

}