use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

use crate::actors::{Actor, ActorConfig, Idle};
use crate::common::{Akton, Broker, BrokerContextType, Context};
use crate::common::akton_inner::AktonInner;

pub struct AktonReady(AktonInner);

impl AktonReady {
    pub fn create<State: Default + Send + Debug>(&mut self) -> Actor<Idle<State>, State> {
        let broker = self.0.broker.clone();

        let config = ActorConfig::new("default", None, Some(broker));
        Actor::new(Some(config), State::default())
    }

    fn spawn_broker() -> BrokerContextType {
        Broker::init()
    }
    pub fn broker(&self) -> BrokerContextType {
        self.0.broker.clone()
    }
    pub async fn spawn_actor<State>(&mut self, setup: impl FnOnce(Actor<Idle<State>, State>) -> Pin<Box<dyn Future<Output=Context> + Send + 'static>>) -> Context
    where
        State: Default + Send + Debug + 'static,
    {
        let broker = self.broker();

        let actor = Actor::new(Some(ActorConfig::new("default", None, Some(broker))), State::default());

        setup(actor).await
        // configured_actor.activate(None)
    }
    fn get_pool_size_from_config() -> usize {
        // TODO: Logic to read from env or config file
        std::env::var("AKTON_BROKER_POOL_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1) // Default value if not set
    }
}

impl From<Akton> for AktonReady {
    fn from(launch: Akton) -> Self {
        let _pool_size = AktonReady::get_pool_size_from_config();

        AktonReady(AktonInner {
            broker: AktonReady::spawn_broker(),
        })
    }
}