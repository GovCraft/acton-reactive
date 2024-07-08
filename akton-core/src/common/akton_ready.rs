use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

use akton_arn::Arn;
use tokio::sync::oneshot;

use crate::actors::{ManagedActor, ActorConfig, Idle};
use crate::common::{Akton, Broker, BrokerRef, ActorRef};
use crate::common::akton_inner::AktonInner;

#[derive(Debug, Clone, Default)]
pub struct AktonReady(pub(crate) AktonInner);

impl AktonReady {
    pub async fn create_actor<State>(&mut self) -> ManagedActor<Idle, State>
    where
        State: Default + Send + Debug + 'static,
    {
        let broker = self.0.broker.clone();
        let akton_ready = self.clone();
        let config = ActorConfig::new(Arn::default(), None, Some(broker)).unwrap_or_default();
        ManagedActor::new(&Some(akton_ready), Some(config), State::default()).await
    }

    pub async fn create_actor_with_config<State>(&mut self, config: ActorConfig) -> ManagedActor<Idle, State>
    where
        State: Default + Send + Debug + 'static,
    {
        let akton_ready = self.clone();
        ManagedActor::new(&Some(akton_ready), Some(config), State::default()).await
    }

    pub fn get_broker(&self) -> BrokerRef {
        self.0.broker.clone()
    }

    pub async fn spawn_actor_with_setup<State>(
        &mut self,
        config: ActorConfig,
        setup_fn: impl FnOnce(ManagedActor<Idle, State>) -> Pin<Box<dyn Future<Output =ActorRef> + Send + 'static>>,
    ) -> anyhow::Result<ActorRef>
    where
        State: Default + Send + Debug + 'static,
    {
        let akton_ready = self.clone();
        let actor = ManagedActor::new(&Some(akton_ready), Some(config), State::default()).await;
        Ok(setup_fn(actor).await)
    }

    pub async fn spawn_actor<State>(
        &mut self,
        setup_fn: impl FnOnce(ManagedActor<Idle, State>) -> Pin<Box<dyn Future<Output =ActorRef> + Send + 'static>>,
    ) -> anyhow::Result<ActorRef>
    where
        State: Default + Send + Debug + 'static,
    {
        let broker = self.get_broker();
        let config = ActorConfig::new(Arn::default(), None, Some(broker.clone()))?;
        let akton_ready = self.clone();
        let actor = ManagedActor::new(&Some(akton_ready), Some(config), State::default()).await;
        Ok(setup_fn(actor).await)
    }

    fn get_pool_size() -> usize {
        std::env::var("AKTON_BROKER_POOL_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1)
    }
}

impl From<Akton> for AktonReady {
    fn from(akton: Akton) -> Self {
        let pool_size = AktonReady::get_pool_size();

        let (sender, receiver) = oneshot::channel();

        tokio::spawn(async move {
            let broker = Broker::initialize().await;
            let _ = sender.send(broker);
        });

        let broker = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                receiver.await.expect("Broker initialization failed")
            })
        });

        AktonReady(AktonInner { broker })
    }
}