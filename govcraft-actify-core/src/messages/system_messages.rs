// use govcraft_actify::traits::message::GovcraftMessage;

use crate::traits::message::GovcraftMessage;

#[derive(Clone,Debug)]
pub enum SystemMessage{
    Start,
    Shutdown,
    Terminate
}

impl GovcraftMessage for SystemMessage {

}

unsafe impl Send for SystemMessage {

}