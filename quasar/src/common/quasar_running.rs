use std::io::Write;
use std::sync::atomic::Ordering;
use std::time::Duration;
use async_trait::async_trait;
use quasar_qrn::Qrn;
use tokio::sync::mpsc::channel;
use tracing::{debug, error, instrument, trace};
use crate::common::{ActorInbox, ActorInboxAddress, ActorReactorMap, ActorStopFlag, LifecycleEventReactor, LifecycleInbox, LifecycleInboxAddress, LifecycleReactorMap, LifecycleStopFlag, QuasarDormant};
use crate::common::*;
use crate::traits::Actor;

pub struct QuasarRunning<T: 'static, U: 'static> {
    pub qrn: Qrn,
    pub state: T,
    pub(crate) lifecycle_message_reactor_map: Option<LifecycleReactorMap<T, U>>,
    lifecycle_inbox: LifecycleInbox,
    pub(crate) lifecycle_inbox_address: LifecycleInboxAddress,
    lifecycle_stop_flag: LifecycleStopFlag,
    on_start_reactor: LifecycleEventReactor<QuasarRunning<T, U>>,
    on_stop_reactor: LifecycleEventReactor<QuasarRunning<T, U>>,
    pub(crate) actor_message_reactor_map: Option<ActorReactorMap<T, U>>,
    actor_inbox: ActorInbox,
    pub(crate) actor_inbox_address: ActorInboxAddress,
    actor_stop_flag: ActorStopFlag,
}

impl<T, U> QuasarRunning<T, U> {
    #[instrument(skip(self, actor_message_reactor_map, lifecycle_message_reactor_map), fields(qrn = & self.qrn.value, self.actor_inbox.is_closed = & self.actor_inbox.is_closed()))]
    pub(crate) async fn actor_listen(&mut self, actor_message_reactor_map: ActorReactorMap<T, U>, lifecycle_message_reactor_map: LifecycleReactorMap<T, U>) {
        (self.on_start_reactor)(self);

        loop {
            trace!("{} items in actor_message_reactor_map", actor_message_reactor_map.len());
            trace!("{} items in lifecycle_message_reactor_map", lifecycle_message_reactor_map.len());
            // tokio::time::sleep(Duration::from_millis(2)).await;
            // Fetch and process actor messages if available
            while let Ok(actor_msg) = self.actor_inbox.try_recv() {
                trace!("actor_msg {:?}", actor_msg);
                let type_id = actor_msg.as_any().type_id();
                if let Some(reactor) = actor_message_reactor_map.get(&type_id) {
                    {
                        reactor(self, &*actor_msg);
                    }
                } else {
                    error!("No handler for message type: {:?}", actor_msg);
                }
            }

            // Check lifecycle messages
            if let Ok(lifecycle_msg) = self.lifecycle_inbox.try_recv() {
                let type_id = lifecycle_msg.as_any().type_id();
                if let Some(reactor) = lifecycle_message_reactor_map.get(&type_id) {
                    reactor(self, &*lifecycle_msg);
                } else {
                    error!("No handler for message type: {:?}", lifecycle_msg);
                }
            }
            else {
                tokio::time::sleep(Duration::from_nanos(1)).await;
            }

            // Check the stop condition after processing messages
            if self.actor_stop_flag.load(Ordering::SeqCst) && self.actor_inbox.is_empty() {
                std::io::stdout().flush().expect("Failed to flush stdout");
                break;
            }
        }
        (self.on_stop_reactor)(self);
    }

    pub(crate) fn stop(&self) {
        if !self.actor_stop_flag.load(Ordering::SeqCst) {
            self.actor_stop_flag.store(true, Ordering::SeqCst);
        }
    }
}


impl<T: Default + Send + Sync, U: Send + Sync> From<Quasar<QuasarDormant<T, U>>> for Quasar<QuasarRunning<T, U>> {
    #[instrument("from dormant to running", skip(value))]
    fn from(value: Quasar<QuasarDormant<T, U>>) -> Quasar<QuasarRunning<T, U>> {
        let (actor_inbox_address, actor_inbox) = channel(255);
        let (lifecycle_inbox_address, lifecycle_inbox) = channel(255);
        debug!("{} items in actor_reactor_map", value.ctx.actor_reactor_map.len());
        Quasar {
            ctx: QuasarRunning {
                lifecycle_inbox_address,
                lifecycle_inbox,
                lifecycle_stop_flag: LifecycleStopFlag::new(false),
                on_start_reactor: value.ctx.on_start_reactor,
                on_stop_reactor: value.ctx.on_stop_reactor,
                actor_message_reactor_map: Some(value.ctx.actor_reactor_map),
                lifecycle_message_reactor_map: Some(value.ctx.lifecycle_reactor_map),
                actor_inbox,
                actor_inbox_address,
                actor_stop_flag: ActorStopFlag::new(false),
                qrn: value.ctx.qrn,
                state: value.ctx.state,
            },
        }
    }
}
#[async_trait]
impl<T: Unpin, U> Actor for QuasarRunning<T, U> {
    type Context = QuasarContext;

    fn get_lifecycle_inbox(&mut self) -> &mut LifecycleInbox {
        &mut self.lifecycle_inbox
    }

    fn get_lifecycle_stop_flag(&mut self) -> &mut LifecycleStopFlag {
        &mut self.lifecycle_stop_flag
    }
}


unsafe impl<T, U> Send for QuasarRunning<T, U> {}
