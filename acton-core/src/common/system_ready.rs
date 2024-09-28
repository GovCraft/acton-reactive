/*
 * Copyright (c) 2024. Govcraft
 *
 * Licensed under either of
 *   * Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *   * MIT license: http://opensource.org/licenses/MIT
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the applicable License for the specific language governing permissions and
 * limitations under that License.
 */

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

use acton_ern::Ern;
use tokio::sync::oneshot;

use crate::actor::{ActorConfig, Idle, ManagedActor};
use crate::common::{ActonSystem, ActorRef, Broker, BrokerRef};
use crate::common::acton_inner::ActonInner;

/// Represents a ready state of the Acton system.
///
/// This struct encapsulates the internal state of the Acton system when it's ready for use.
/// It provides methods for creating and managing actors within the system.
#[derive(Debug, Clone, Default)]
pub struct SystemReady(pub(crate) ActonInner);

impl SystemReady {
    /// Creates a new actor with default configuration.
    ///
    /// # Type Parameters
    ///
    /// * `State` - The state type of the actor, which must implement `Default`, `Send`, `Debug`, and have a static lifetime.
    ///
    /// # Returns
    ///
    /// A `ManagedActor` in the `Idle` state with the specified `State`.
    pub async fn create_actor<State>(&mut self) -> ManagedActor<Idle, State>
    where
        State: Default + Send + Debug + 'static,
    {
        let broker = self.0.broker.clone();
        let acton_ready = self.clone();
        let config = ActorConfig::new(Ern::default(), None, Some(broker)).unwrap_or_default();
        ManagedActor::new(&Some(acton_ready), Some(config)).await
    }

    /// Creates a new actor with a specified configuration.
    ///
    /// # Type Parameters
    ///
    /// * `State` - The state type of the actor, which must implement `Default`, `Send`, `Debug`, and have a static lifetime.
    ///
    /// # Arguments
    ///
    /// * `config` - The `ActorConfig` to use for creating the actor.
    ///
    /// # Returns
    ///
    /// A `ManagedActor` in the `Idle` state with the specified `State` and configuration.
    pub async fn create_actor_with_config<State>(
        &mut self,
        config: ActorConfig,
    ) -> ManagedActor<Idle, State>
    where
        State: Default + Send + Debug + 'static,
    {
        let acton_ready = self.clone();
        ManagedActor::new(&Some(acton_ready), Some(config)).await
    }

    /// Retrieves the broker reference for the system.
    ///
    /// # Returns
    ///
    /// A clone of the `BrokerRef` associated with this `SystemReady` instance.
    pub fn get_broker(&self) -> BrokerRef {
        self.0.broker.clone()
    }

    /// Spawns an actor with a custom setup function and configuration.
    ///
    /// # Type Parameters
    ///
    /// * `State` - The state type of the actor, which must implement `Default`, `Send`, `Debug`, and have a static lifetime.
    ///
    /// # Arguments
    ///
    /// * `config` - The `ActorConfig` to use for creating the actor.
    /// * `setup_fn` - A function that takes a `ManagedActor` and returns a `Future` resolving to an `ActorRef`.
    ///
    /// # Returns
    ///
    /// A `Result` containing the `ActorRef` of the spawned actor, or an error if the spawn failed.
    pub async fn spawn_actor_with_setup<State>(
        &mut self,
        config: ActorConfig,
        setup_fn: impl FnOnce(
            ManagedActor<Idle, State>,
        ) -> Pin<Box<dyn Future<Output=ActorRef> + Send + 'static>>,
    ) -> anyhow::Result<ActorRef>
    where
        State: Default + Send + Debug + 'static,
    {
        let acton_ready = self.clone();
        let actor = ManagedActor::new(&Some(acton_ready), Some(config)).await;
        Ok(setup_fn(actor).await)
    }

    /// Spawns an actor with a custom setup function and default configuration.
    ///
    /// # Type Parameters
    ///
    /// * `State` - The state type of the actor, which must implement `Default`, `Send`, `Debug`, and have a static lifetime.
    ///
    /// # Arguments
    ///
    /// * `setup_fn` - A function that takes a `ManagedActor` and returns a `Future` resolving to an `ActorRef`.
    ///
    /// # Returns
    ///
    /// A `Result` containing the `ActorRef` of the spawned actor, or an error if the spawn failed.
    pub async fn spawn_actor<State>(
        &mut self,
        setup_fn: impl FnOnce(
            ManagedActor<Idle, State>,
        ) -> Pin<Box<dyn Future<Output=ActorRef> + Send + 'static>>,
    ) -> anyhow::Result<ActorRef>
    where
        State: Default + Send + Debug + 'static,
    {
        let broker = self.get_broker();
        let config = ActorConfig::new(Ern::default(), None, Some(broker.clone()))?;
        let acton_ready = self.clone();
        let actor = ManagedActor::new(&Some(acton_ready), Some(config)).await;
        Ok(setup_fn(actor).await)
    }
}

impl From<ActonSystem> for SystemReady {
    fn from(_acton: ActonSystem) -> Self {


        let (sender, receiver) = oneshot::channel();

        tokio::spawn(async move {
            let broker = Broker::initialize().await;
            let _ = sender.send(broker);
        });

        let broker = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { receiver.await.expect("Broker initialization failed") })
        });

        SystemReady(ActonInner { broker })
    }
}
