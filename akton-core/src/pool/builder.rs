/*
 *
 *  *
 *  * Copyright (c) 2024 Govcraft.
 *  *
 *  *  Licensed under the Business Source License, Version 1.1 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *      https://github.com/GovCraft/akton-framework/tree/main/LICENSES
 *  *
 *  *  Change Date: Three years from the release date of this version of the Licensed Work.
 *  *  Change License: Apache License, Version 2.0
 *  *
 *  *  Usage Limitations:
 *  *    - You may use the Licensed Work for non-production purposes only, such as internal testing, development, and experimentation.
 *  *    - You may not use the Licensed Work for any production or commercial purpose, including, but not limited to, the provision of any service to third parties, without a commercial use license from the Licensor, except as stated in the Exemptions section of the License.
 *  *
 *  *  Exemptions:
 *  *    - Open Source Projects licensed under an OSI-approved open source license.
 *  *    - Non-Profit Organizations using the Licensed Work for non-commercial purposes.
 *  *    - Small For-Profit Companies with annual gross revenues not exceeding $2,000,000 USD.
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */

use std::collections::HashMap;
use std::fmt::Debug;

use dashmap::DashMap;
use tracing::{info, instrument, trace};
use crate::actors::ActorConfig;

use crate::common::{Context, LoadBalanceStrategy};
use crate::pool::{PoolConfig, PoolItem, RandomStrategy, RoundRobinStrategy};
use crate::traits::{LoadBalancerStrategy, PooledActor};

/// Builder for creating and configuring actor pools.
#[derive(Debug, Default)]
pub struct PoolBuilder {
    /// A map of pool names to their configurations.
    pools: HashMap<String, PoolConfig>,
}

impl PoolBuilder {
    /// Adds a new pool to the builder.
    ///
    /// # Parameters
    /// - `name`: The name of the pool.
    /// - `size`: The size of the pool (number of actors).
    /// - `strategy`: The load balancing strategy for the pool.
    ///
    /// # Returns
    /// The updated `PoolBuilder` instance.
    pub fn add_pool<T: PooledActor + Default + Debug + Send + 'static>(
        mut self,
        name: &str,
        size: usize,
        strategy: LoadBalanceStrategy,
    ) -> Self {
        let pool = T::default();
        let def = PoolConfig {
            size,
            actor_type: Box::new(pool),
            strategy,
        };
        self.pools.insert(name.to_string(), def);
        self
    }

    /// Spawns the supervisor and initializes the actor pools.
    ///
    /// # Parameters
    /// - `parent`: The parent context for the actors.
    ///
    /// # Returns
    /// A new `Supervisor` instance.
    #[instrument(skip(self, parent), fields(id=parent.key.value))]
    pub(crate) async fn spawn(mut self, parent: &Context) -> anyhow::Result<DashMap<String, PoolItem>> {
        let subordinates = DashMap::new();

        for (pool_name, pool_def) in &mut self.pools {
            let pool_name = pool_name.to_string();
            let mut context_items = Vec::with_capacity(pool_def.size);

            for i in 0..pool_def.size {
                let item_name = format!("{}{}", pool_name, i);
                let actor_config = ActorConfig::new (
                    item_name.clone(),
                    None,
                    None,
                                                         );
                let context = pool_def
                    .actor_type
                    .initialize(actor_config)
                    .await;

                // Event: Actor Initialized
                // Description: An actor has been initialized and added to the pool.
                // Context: Item name and context details.
                trace!(item_name = %item_name, context = ?context, "Actor initialized and added to the pool.");
                context_items.push(context);
            }

            // Select the appropriate load balancing strategy.
            let strategy: Box<dyn LoadBalancerStrategy> = match &pool_def.strategy {
                LoadBalanceStrategy::RoundRobin => Box::<RoundRobinStrategy>::default(),
                LoadBalanceStrategy::Random => Box::<RandomStrategy>::default(),
                LoadBalanceStrategy::LeastBusy => unimplemented!(),
                LoadBalanceStrategy::HashBased => unimplemented!(),
            };

            // Create a pool item and insert it into the subordinates map.
            let item = PoolItem {
                id: pool_name.clone(),
                pool: context_items,
                strategy,
            };
            subordinates.insert(pool_name, item);
        }

        // Event: Supervisor Initialized
        // Description: The supervisor has been initialized with the actor pools.
        // Context: Parent key and number of subordinates.
        info!(parent_key = %parent.key.value, subordinates_count = subordinates.len(), "Supervisor initialized with actor pools.");

        // Return the new supervisor instance.
        Ok(subordinates)
    }
}
