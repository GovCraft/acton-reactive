use std::fmt::Debug;
use std::future::Future;

use tokio::runtime::Runtime;

use crate::actors::{Actor, ActorConfig, Idle};
use crate::common::{Broker, BrokerContextType, Context};
use crate::common::akton_inner::AktonInner;
use crate::common::akton_launch::AktonLaunch;

pub struct AktonReady(AktonInner);

impl AktonReady {
    pub fn create<State: Default + Send + Debug>(&mut self) -> Actor<Idle<State>, State> {
        let broker = self.0.broker_pool.pop()
            .expect("Broker pool is empty");

        let config = ActorConfig::new("default", None, Some(broker));
        Actor::new(Some(config), State::default())
    }

    async fn spawn_broker() -> anyhow::Result<BrokerContextType> {
        let broker_context = Broker::init().await?;
        Ok(broker_context)
    }
    pub async fn spawn_actor<State, F, Fut>(&mut self, setup: F) -> anyhow::Result<Context>
    where
        State: Default + Send + Debug + 'static,
        F: FnOnce(Actor<Idle<State>, State>) -> Fut + Send + 'static,
        Fut: Future<Output=anyhow::Result<Actor<Idle<State>, State>>> + Send + 'static,
    {
        self.0.runtime.spawn(async move {
            // Create the initial actor
            let actor = Actor::new(None, State::default());

            // Call the setup closure to allow customization
            let configured_actor = setup(actor).await?;

            // Activate the actor
            configured_actor.activate(None).await
        }).await?
    }
    fn get_pool_size_from_config() -> usize {
        // TODO: Logic to read from env or config file
        std::env::var("AKTON_BROKER_POOL_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1) // Default value if not set
    }
}

impl From<AktonLaunch> for AktonReady {
    fn from(launch: AktonLaunch) -> Self {
        let runtime = launch.runtime.unwrap_or_else(|| Runtime::new().expect("Failed to create Tokio runtime"));

        let broker_pool = runtime.block_on(async {
            let pool_size = AktonReady::get_pool_size_from_config();
            let mut brokers = Vec::with_capacity(pool_size);
            for _ in 0..pool_size {
                if let Ok(broker) = AktonReady::spawn_broker().await {
                    brokers.push(broker);
                }
            }
            brokers
        });

        AktonReady(AktonInner { runtime, broker_pool })
    }
}