use std::io::Write;
use std::sync::atomic::Ordering;
use async_trait::async_trait;
use quasar_qrn::Qrn;
use tokio::sync::mpsc::channel;
use crate::common::{ActorInbox, ActorInboxAddress, ActorReactorMap, ActorStopFlag, LifecycleEventReactor, LifecycleEventReactorMut, LifecycleInbox, LifecycleInboxAddress, LifecycleReactorMap, LifecycleStopFlag, QuasarDormant};
use crate::common::*;
use crate::traits::Actor;

pub struct QuasarRunning<T:'static, U:'static> {
    pub qrn: Qrn,
    pub state: Option<&'static T>,
    supervisor: Option<&'static QuasarRunning<T,U>>,
    pub(crate) lifecycle_message_reactor_map: Option<LifecycleReactorMap<T,U>>,
    lifecycle_inbox: LifecycleInbox,
    pub(crate) lifecycle_inbox_address: LifecycleInboxAddress,
    lifecycle_stop_flag: LifecycleStopFlag,
    on_start_reactor: LifecycleEventReactor<QuasarRunning<T,U>>,
    on_stop_reactor: LifecycleEventReactor<QuasarRunning<T,U>>,
    on_before_message_receive_reactor: LifecycleEventReactorMut<T,U>,
    on_after_message_receive_reactor: LifecycleEventReactor<QuasarRunning<T,U>>,
    pub(crate) actor_message_reactor_map: Option<ActorReactorMap<T,U>>,
    actor_inbox: ActorInbox,
    pub(crate) actor_inbox_address: ActorInboxAddress,
    actor_stop_flag: ActorStopFlag,
}

impl<T,U> QuasarRunning<T, U> {

    pub(crate) async fn actor_listen(&mut self, actor_message_reactor_map: ActorReactorMap<T, U>, lifecycle_message_reactor_map: LifecycleReactorMap<T, U>) {
        let _ = (self.on_start_reactor)(self);
        loop {
            // Fetch and process actor messages if available
            while let Ok(actor_msg) = self.actor_inbox.try_recv() {
                let type_id = actor_msg.as_any().type_id();
                if let Some(reactor) = actor_message_reactor_map.get(&type_id) {
                    {
                        (&self.on_before_message_receive_reactor)(self, &*actor_msg);
                    }
                    reactor(self, &*actor_msg);
                    // (self.on_after_message_receive_reactor)(self);
                } else {
                    eprintln!("No handler for message type: {:?}", actor_msg);
                }
            }

            // Check lifecycle messages
            if let Ok(lifecycle_msg) = self.lifecycle_inbox.try_recv() {
                let type_id = lifecycle_msg.as_any().type_id();
                if let Some(reactor) = lifecycle_message_reactor_map.get(&type_id) {
                    reactor(self, &*lifecycle_msg);
                } else {
                    eprintln!("No handler for message type: {:?}", lifecycle_msg);
                }
            }

            // Check the stop condition after processing messages
            if self.actor_stop_flag.load(Ordering::SeqCst) && self.actor_inbox.is_empty() {
                std::io::stdout().flush().expect("Failed to flush stdout");
                break;
            }
        }
        let _ = (self.on_stop_reactor)(self);
    }

    pub(crate) fn stop(&self) {
        if !self.actor_stop_flag.load(Ordering::SeqCst) {
            self.actor_stop_flag.store(true, Ordering::SeqCst);
        }
    }
}


impl<T,U> From<Quasar<QuasarDormant<T, U>>> for Quasar<QuasarRunning<T, U>> {
    fn from(value: Quasar<QuasarDormant<T,U>>) -> Quasar<QuasarRunning<T,U>> {
        let (actor_inbox_address, actor_inbox) = channel(255);
        let (lifecycle_inbox_address, lifecycle_inbox) = channel(255);

        Quasar {
            ctx: QuasarRunning {
                lifecycle_inbox_address,
                lifecycle_inbox,
                lifecycle_stop_flag: LifecycleStopFlag::new(false),
                on_start_reactor: value.ctx.on_start_reactor,
                on_stop_reactor: value.ctx.on_stop_reactor,
                on_before_message_receive_reactor: value.ctx.on_before_message_receive_reactor,
                on_after_message_receive_reactor: value.ctx.on_after_message_receive_reactor,
                actor_message_reactor_map: Some(value.ctx.actor_reactor_map),
                lifecycle_message_reactor_map: Some(value.ctx.lifecycle_reactor_map),
                actor_inbox,
                actor_inbox_address,
                actor_stop_flag: ActorStopFlag::new(false),
                qrn: value.ctx.qrn,
                state: None,
                supervisor: None,
            },
        }
    }
}

#[async_trait]
impl<T,U> Actor for QuasarRunning<T, U> {
    type Context = QuasarContext;

    fn get_lifecycle_inbox(&mut self) -> &mut LifecycleInbox {
        &mut self.lifecycle_inbox
    }

    fn get_lifecycle_stop_flag(&mut self) -> &mut LifecycleStopFlag {
        &mut self.lifecycle_stop_flag
    }
}


unsafe impl<T,U> Send for QuasarRunning<T,U> {}
