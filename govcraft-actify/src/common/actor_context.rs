use tokio::sync::mpsc::Sender;
use govcraft_actify_core::messages::{SupervisorMessage, SystemMessage};
use crate::prelude::broadcast::Receiver as GroupReceiver;
use crate::prelude::broadcast::Sender as GroupPost;
use crate::prelude::mpsc::Receiver;
use std::fmt;

pub type Mailbox<T>= Receiver<T>;
pub type AdminMailbox<SupervisorMessage> = Receiver<SupervisorMessage>;
pub type AdminPost<SupervisorMessage> = Sender<SupervisorMessage>;
pub type DirectPost<T> = Sender<T>;
pub type GroupMailbox<T> = GroupReceiver<T>;
// #[derive(Debug)]
pub struct ActorContext<T> {
    // pub(crate) group_mailbox: GroupMailbox<T>,
    pub(crate) admin_mailbox: AdminMailbox<SystemMessage>,
    pub(crate) admin_post: AdminPost<SystemMessage>,
    pub(crate) mailbox: Mailbox<T>,
    pub(crate) direct_post: DirectPost<T>,

}
impl<T> fmt::Debug for ActorContext<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorContext")
            .field("admin_mailbox", &format_args!("<AdminMailbox>"))
            .field("admin_post", &format_args!("<AdminPost>"))
            .field("mailbox", &format_args!("<Mailbox>"))
            .field("direct_post", &format_args!("<DirectPost>"))
            .finish()
    }
}

impl<T> fmt::Debug for BroadcastContext<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BroadcastContext")
            .field("group_mailbox", &format_args!("<GroupMailbox>"))
            .field("group_post", &format_args!("<GroupPost>"))
            .finish()
    }
}

// #[derive(Debug)]
pub struct BroadcastContext<T> {
    pub(crate) group_mailbox: GroupMailbox<T>,
    pub(crate) group_post: GroupPost<T>,

}