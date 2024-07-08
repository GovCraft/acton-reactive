use std::fmt::Debug;

use dashmap::DashMap;
use tokio::sync::mpsc::channel;

use crate::actors::ManagedActor;
use crate::common::ActorRef;

pub struct Idle;

impl<ManagedEntity: Default + Send + Debug + 'static> Default for ManagedActor<Idle, ManagedEntity> {
    fn default() -> Self {
        let (outbox, inbox) = channel(255);
        let mut actor_ref: ActorRef = Default::default();
        actor_ref.outbox = Some(outbox.clone());

        ManagedActor::<Idle, ManagedEntity> {
            actor_ref,
            parent: Default::default(),
            key: Default::default(),
            entity: ManagedEntity::default(),
            broker: Default::default(),
            inbox,
            akton: Default::default(),
            halt_signal: Default::default(),
            tracker: Default::default(),
            before_activate: Box::new(|_| {}),
            on_activate: Box::new(|_| {}),
            before_stop: Box::new(|_| {}),
            on_stop: Box::new(|_| {}),
            before_stop_async: None,
            reactors: DashMap::new(),

            _actor_state: Default::default(),
        }
    }
}
