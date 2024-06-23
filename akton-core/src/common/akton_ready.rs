use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use akton_arn::Arn;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::actors::{Actor, ActorConfig, Idle};
use crate::common::{Akton, Broker, BrokerContextType, Context};
use crate::common::akton_inner::AktonInner;

#[derive(Debug, Clone)]
pub struct AktonReady(pub(crate) AktonInner);

impl AktonReady {
    pub async fn create<State: Default + Send + Debug + 'static>(&mut self) -> Actor<Idle<State>, State> {
        let broker = self.0.broker.clone();
        let akton = self.clone();
        let config = ActorConfig::new(Arn::default(), None, Some(broker)).unwrap_or_default();
        Actor::new(&Some(akton), Some(config), State::default()).await
    }
    pub async fn create_with_config<State: Default + Send + Debug + 'static>(&mut self, config: ActorConfig) -> Actor<Idle<State>, State> {
        let akton = self.clone();
        Actor::new(&Some(akton), Some(config), State::default()).await
    }

    pub fn broker(&self) -> BrokerContextType {

        self.0.broker.clone()
    }
    // fn spawn_broker(&self) -> BrokerContextType {
    //
    //     self.0.broker.clone()
    // }

    pub async fn spawn_actor_with_config<State>(&mut self, config: ActorConfig, setup: impl FnOnce(Actor<Idle<State>, State>) -> Pin<Box<dyn Future<Output=Context> + Send + 'static>>) -> anyhow::Result<Context>
    where
        State: Default + Send + Debug + 'static,
    {
        let akton = self.clone();
        let actor = Actor::new(&Some(akton), Some(config), State::default()).await;


        Ok(setup(actor).await)
    }

    pub async fn spawn_actor<State>(&mut self, setup: impl FnOnce(Actor<Idle<State>, State>) -> Pin<Box<dyn Future<Output=Context> + Send + 'static>>) -> anyhow::Result<Context>
    where
        State: Default + Send + Debug + 'static,
    {
        let broker = self.broker();
        let broker = broker.clone();
        let akton = self.clone();

        let actor = Actor::new(&Some(akton), Some(ActorConfig::new(Arn::default(), None, Some(broker))?), State::default()).await;


        Ok(setup(actor).await)
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
        // TODO: This will be a broker pool in a future release
        let _pool_size = AktonReady::get_pool_size_from_config();

        // Create a channel for receiving the initialized broker
        let (tx, rx) = oneshot::channel();

        // Spawn the broker initialization task
        tokio::spawn(async move {
            let broker = Broker::init().await;
            let _ = tx.send(broker);
        });

        // Wait for the broker to be initialized
        let broker = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                rx.await.expect("Broker initialization failed")
            })
        });

        AktonReady(AktonInner {
            broker
        })
    }
}