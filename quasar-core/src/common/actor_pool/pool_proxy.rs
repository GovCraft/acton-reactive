/*
 *
 *  * Copyright (c) 2024 Govcraft.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

use std::any::Any;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use async_trait::async_trait;
use dashmap::DashMap;
use tracing::{debug, instrument};
use crate::common::{Context, ContextPool, System};
use crate::common::actor_pool::messages::{AssignPool, NewPoolMessage};
use crate::traits::{ConfigurableActor, QuasarMessage};


#[derive(Default, Debug)]
pub struct PoolProxy<T> {
    pub pool: ContextPool,
    _phantom: PhantomData<T>,
}




#[async_trait]
impl<T: ConfigurableActor + 'static> ConfigurableActor for PoolProxy<T> {
    #[instrument]
    async fn init(name: String, _context:&Context) -> Context {
        let name = name.to_owned();
        let mut proxy = System::<PoolProxy<T>>::new_actor(PoolProxy::default());
        proxy.ctx.act_on_async::<NewPoolMessage>(move |actor, record| {
            debug!("new pool message received with pool size {}", record.message.size);
            let size = record.message.size;
            let local_name = name.clone();
            let envelope = actor.new_envelope();
            // Create and immediately return the async task
            Box::pin(async move {
                // Perform the local task spawn and map collection within an async block
                debug!("new pool message entering task");
                // let task = tokio::task::spawn_local(async move {
                    debug!("new pool message spawning");
                    let mut items = DashMap::new();  // DashMap to store the results
                    for i in 0..size {
                        let actor_name = format!("{}{}", local_name, i);
                        debug!("adding actor to pool: {}", actor_name);
                        // let pool_item = T::init(&actor_name).await;
                        // items.insert(pool_item.key.value.clone(), pool_item);  // Collect each result
                    }
                    if let Some(envelope) = envelope {
                        envelope.reply(AssignPool { pool: items }).await;
                    }
                // });
                // debug!("awaiting task");
                // match task.await {
                //     Ok(_) => {
                //         debug!("Spawned task completed")
                //     }
                //     Err(_) => {
                //         debug!("Spawned task failed")
                //     }
                // }
            })
        })
            .act_on::<AssignPool>(|actor, record| {
                debug!("assign pool message received");
                let pool = record.message.pool.clone();
                actor.state.pool = pool;
            });

        proxy.spawn().await
    }
}


