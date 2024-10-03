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
use futures::future::join_all;
use tokio::sync::oneshot;
use tracing::trace;

use crate::actor::{AgentConfig, Idle, ManagedAgent};
use crate::common::{ActonApp, AgentBroker, AgentHandle, BrokerRef};
use crate::common::acton_inner::ActonInner;
use crate::traits::Actor;

/// Represents a ready state of the Acton system.
///
/// This struct encapsulates the internal state of the Acton system when it's ready for use.
/// It provides methods for creating and managing actors within the system.
#[derive(Debug, Clone, Default)]
pub struct AgentRuntime(pub(crate) ActonInner);

impl AgentRuntime {
    /// Creates a new actor with the provided id root name.
    ///
    /// # Type Parameters
    ///
    /// * `State` - The state type of the actor, which must implement `Default`, `Send`, `Debug`, and have a static lifetime.
    ///
    /// # Returns
    ///
    /// A `ManagedActor` in the `Idle` state with the specified `State`.
    pub async fn new_agent_with_name<State>(&mut self, name: String) -> ManagedAgent<Idle, State>
    where
        State: Default + Send + Debug + 'static,
    {
        let actor_config = AgentConfig::new(
            Ern::with_root(name).unwrap(),
            None,
            Some(self.0.broker.clone()),
        ).expect("Failed to create actor config");

        // let broker = self.0.broker.clone();
        let runtime = self.clone();
        let new_actor = ManagedAgent::new(&Some(runtime), Some(actor_config)).await;
        self.0.roots.insert(new_actor.id.clone(), new_actor.handle.clone());
        new_actor
    }

    /// Creates a new actor with default configuration.
    ///
    /// # Type Parameters
    ///
    /// * `State` - The state type of the actor, which must implement `Default`, `Send`, `Debug`, and have a static lifetime.
    ///
    /// # Returns
    ///
    /// A `ManagedActor` in the `Idle` state with the specified `State`.
    pub async fn new_agent<State>(&mut self) -> ManagedAgent<Idle, State>
    where
        State: Default + Send + Debug + 'static,
    {
        let actor_config = AgentConfig::new(
            Ern::with_root("agent").unwrap(),
            None,
            Some(self.0.broker.clone()),
        ).expect("Failed to create actor config");

        // let broker = self.0.broker.clone();
        let runtime = self.clone();
        let new_actor = ManagedAgent::new(&Some(runtime), Some(actor_config)).await;
        self.0.roots.insert(new_actor.id.clone(), new_actor.handle.clone());
        new_actor
    }

    /// Retrieves the number of actors currently running in the system.
    pub fn agent_count(&self) -> usize {
        self.0.roots.len()
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
        mut config: AgentConfig,
    ) -> ManagedAgent<Idle, State>
    where
        State: Default + Send + Debug + 'static,
    {
        let acton_ready = self.clone();
        // we should make sure the config has a broker, if it doesn't, we should provide it from self.0.broker
        if config.broker.is_none() {
            config.broker = Some(self.0.broker.clone());
        }
        let new_agent = ManagedAgent::new(&Some(acton_ready), Some(config)).await;
        trace!("Created new actor with id {}", new_agent.id);
        self.0.roots.insert(new_agent.id.clone(), new_agent.handle.clone());
        new_agent
    }

    /// Retrieves the broker reference for the system.
    ///
    /// # Returns
    ///
    /// A clone of the `BrokerRef` associated with this `SystemReady` instance.
    pub fn broker(&self) -> BrokerRef {
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
    pub async fn spawn_agent_with_setup_fn<State>(
        &mut self,
        mut config: AgentConfig,
        setup_fn: impl FnOnce(
            ManagedAgent<Idle, State>,
        ) -> Pin<Box<dyn Future<Output=AgentHandle> + Send + 'static>>,
    ) -> anyhow::Result<AgentHandle>
    where
        State: Default + Send + Debug + 'static,
    {
        let acton_ready = self.clone();
        if config.broker.is_none() {
            config.broker = Some(self.0.broker.clone());
        }

        let new_agent = ManagedAgent::new(&Some(acton_ready), Some(config)).await;
        let handle = setup_fn(new_agent).await;
        self.0.roots.insert(handle.id.clone(), handle.clone());
        Ok(handle)
    }

    /// Shuts down the Acton system, stopping all actors and their children.
    pub async fn shutdown_all(&mut self) -> anyhow::Result<()> {
        // Collect all suspend futures into a vector
        let suspend_futures = self.0.roots.iter().map(|item| {
            let root_actor = item.value().clone(); // Clone to take ownership
            async move {
                root_actor.stop().await
            }
        });

        // Wait for all actors to suspend concurrently
        let results: Vec<anyhow::Result<()>> = join_all(suspend_futures).await;

        // Check for any errors
        for result in results {
            result?;
        }
        self.0.broker.stop().await?;

        Ok(())
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
            ManagedAgent<Idle, State>,
        ) -> Pin<Box<dyn Future<Output=AgentHandle> + Send + 'static>>,
    ) -> anyhow::Result<AgentHandle>
    where
        State: Default + Send + Debug + 'static,
    {
        let broker = self.broker();
        let mut config = AgentConfig::new(Ern::default(), None, Some(broker.clone()))?;
        if config.broker.is_none() {
            config.broker = Some(self.0.broker.clone());
        }
        let runtime = self.clone();
        let new_agent = ManagedAgent::new(&Some(runtime), Some(config)).await;
        let handle = setup_fn(new_agent).await;
        self.0.roots.insert(handle.id.clone(), handle.clone());
        Ok(handle)
    }
}

impl From<ActonApp> for AgentRuntime {
    fn from(_acton: ActonApp) -> Self {
        let (sender, receiver) = oneshot::channel();

        tokio::spawn(async move {
            let broker = AgentBroker::initialize().await;
            let _ = sender.send(broker);
        });

        let broker = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { receiver.await.expect("Broker initialization failed") })
        });

        AgentRuntime(ActonInner { broker: broker.clone(), ..Default::default() })
    }
}
