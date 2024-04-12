use crate::traits::message::GovcraftMessage;

#[derive(Clone,Debug)]
pub enum SystemMessage{
    Start
}

impl GovcraftMessage for SystemMessage {

}

unsafe impl Send for SystemMessage {

}