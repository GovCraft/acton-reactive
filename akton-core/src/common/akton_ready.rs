use std::fmt::Debug;
use std::future::Future;

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
    pub async fn spawn_actor<State, F, Fut>(&mut self, setup: F) -> Context
    where
        State: Default + Send + Debug + 'static,
        F: FnOnce(Actor<Idle<State>, State>) -> Fut + Send + 'static,
        Fut: Future<Output=Actor<Idle<State>, State>> + Send + 'static,
    {
        // Create the initial actor
        let actor = Actor::new(None, State::default());

        // Call the setup closure to allow customization
        let configured_actor = setup(actor).await;

        // Activate the actor
        configured_actor.activate(None)
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