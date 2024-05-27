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

use crate::common::StopSignal;
use crate::common::*;
use crate::prelude::{ActorContext, ConfigurableActor, LoadBalancerStrategy, SupervisorContext};
use dashmap::DashMap;
use akton_arn::Arn;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_util::task::TaskTracker;
use tracing::instrument;

#[derive(Debug)]
pub(crate) struct PoolDef {
    pub(crate) size: usize,
    pub(crate) actor_type: Box<dyn ConfigurableActor>,
    pub(crate) strategy: LBStrategy,
}

#[derive(Debug, Default)]
pub struct PoolBuilder {
    pools: HashMap<String, PoolDef>,
}
impl PoolBuilder {
    pub fn add_pool<T: ConfigurableActor + Default + Debug + Send + 'static>(
        mut self,
        name: &str,
        size: usize,
        strategy: LBStrategy,
    ) -> Self {
        let pool = T::default();
        let def = PoolDef {
            size,
            actor_type: Box::new(pool),
            strategy,
        };
        self.pools.insert(name.to_string(), def);
        self
    }

    #[instrument(skip(self, parent), fields(id=parent.key.value))]
    pub(crate) async fn spawn(mut self, parent: &Context) -> Supervisor {
        let subordinates = DashMap::new();
        for (pool_name, pool_def) in &mut self.pools {
            let pool_name = pool_name.to_string();
            let mut context_items = Vec::with_capacity(pool_def.size);
            for i in 0..pool_def.size {
                let item_name = format!("{}{}", pool_name, i);
                let context = pool_def.actor_type.init(item_name.clone(), parent).await;
                tracing::info!("item_name: {}, context: {:?}", &item_name, &context);
                context_items.push(context);
            }
            let strategy: Box<dyn LoadBalancerStrategy> = match &pool_def.strategy {
                LBStrategy::RoundRobin => Box::<RoundRobinStrategy>::default(),
                LBStrategy::Random => Box::<RandomStrategy>::default(),
            };
            let item = PoolItem {
                pool: context_items,
                strategy,
            };
            subordinates.insert(pool_name, item);
        }
        let (outbox, mailbox) = channel(255);
        let task_tracker = TaskTracker::new();
        Supervisor {
            key: parent.key.clone(),
            halt_signal: StopSignal::new(false),
            subordinates,
            outbox,
            mailbox,
            task_tracker,
        }
    }
}
pub(crate) struct Supervisor {
    pub(crate) key: Arn,
    pub(crate) halt_signal: StopSignal,
    pub(crate) subordinates: DashMap<String, PoolItem>,
    pub(crate) task_tracker: TaskTracker,
    pub(crate) outbox: Sender<Envelope>,
    pub(crate) mailbox: Receiver<Envelope>,
}
impl Debug for Supervisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.key.value)
    }
}

#[derive(Debug)]
pub(crate) struct PoolItem {
    pub(crate) pool: Vec<Context>,
    pub(crate) strategy: Box<dyn LoadBalancerStrategy>,
}

impl Supervisor {
    #[instrument(skip(self))]
    pub(crate) async fn wake_supervisor(&mut self) -> anyhow::Result<()> {
        loop {
            if let Ok(envelope) = self.mailbox.try_recv() {
                if let Some(ref pool_id) = envelope.pool_id {
                    tracing::trace!("{:?}", self.subordinates);
                    if let Some(mut pool_def) = self.subordinates.get_mut(pool_id) {
                        // First, clone or copy the data needed for the immutable borrow.
                        // NOTE: Cloning the whole pool may be expensive, so consider alternatives if performance is a concern.
                        let pool_clone = pool_def.pool.clone();

                        // Now perform the selection outside of the mutable borrow's scope.
                        if let Some(index) = pool_def.strategy.select_item(&pool_clone) {
                            // Access the original data using the index now that we're outside the conflicting borrow.
                            let context = &pool_def.pool[index];
                            context.emit_envelope(envelope).await?;
                        }
                    }
                } else if let Some(concrete_msg) =
                    envelope.message.as_any().downcast_ref::<SystemSignal>()
                {
                    match concrete_msg {
                        SystemSignal::Wake => {}
                        SystemSignal::Recreate => {}
                        SystemSignal::Suspend => {}
                        SystemSignal::Resume => {}
                        SystemSignal::Terminate => {
                            self.terminate().await?;
                        }
                        SystemSignal::Supervise => {}
                        SystemSignal::Watch => {}
                        SystemSignal::Unwatch => {}
                        SystemSignal::Failed => {}
                    }
                } // Checking stop condition .
            }
            let should_stop =
                { self.halt_signal.load(Ordering::SeqCst) && self.mailbox.is_empty() };

            if should_stop {
                break;
            } else {
                tokio::time::sleep(Duration::from_nanos(1)).await;
            }
        }
        Ok(())
    }
    #[instrument(skip(self))]
    pub(crate) async fn terminate(&self) -> anyhow::Result<()> {
        let subordinates = &self.subordinates;
        tracing::trace!("subordinate count: {}", subordinates.len());
        let halt_signal = self.halt_signal.load(Ordering::SeqCst);
        if !halt_signal {
            for item in subordinates {
                for context in &item.value().pool {
                    let envelope = &context.return_address();
                    //                    tracing::warn!("Terminating {}", &context.key.value);
                    tracing::trace!("Terminating done {:?}", &context);
                    //                        if let Some(envelope) = supervisor {
                    envelope.reply(SystemSignal::Terminate, None)?;
                    //                       }
                    //context.terminate_subordinates().await;
                    context.terminate_actor().await?;
                }
            }
            self.halt_signal.store(true, Ordering::SeqCst);
        }
        Ok(())
    }
}
