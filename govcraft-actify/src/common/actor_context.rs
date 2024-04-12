use tokio::sync::mpsc::Sender;
use crate::messages::SupervisorMessage;
use crate::prelude::broadcast::Receiver as GroupReceiver;
use crate::prelude::broadcast::Sender as GroupPost;
use crate::prelude::mpsc::Receiver;
use crate::traits::actor::DirectMessageHandler;

pub type Mailbox<T>= Receiver<T>;
pub type AdminMailbox<SupervisorMessage> = Receiver<SupervisorMessage>;
pub type AdminPost<SupervisorMessage> = Sender<SupervisorMessage>;
pub type DirectPost<T> = Sender<T>;
pub type GroupMailbox<T> = GroupReceiver<T>;
pub struct ActorContext<T> {
    // pub(crate) group_mailbox: GroupMailbox<T>,
    pub(crate) admin_mailbox: AdminMailbox<SupervisorMessage>,
    pub(crate) admin_post: AdminPost<SupervisorMessage>,
    pub(crate) mailbox: Mailbox<T>,
    pub(crate) direct_post: DirectPost<T>,

}

pub struct BroadcastContext<T> {
    pub(crate) group_mailbox: GroupMailbox<T>,
    pub(crate) group_post: GroupPost<T>,

}