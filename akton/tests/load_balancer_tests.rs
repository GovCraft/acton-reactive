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

mod setup;
use akton::prelude::*;
use crate::setup::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_actor_pool_round_robin() -> anyhow::Result<()> {
    // Initialize tracing for logging purposes
    init_tracing();

    // Create the main actor
    let mut main_actor = Akton::<PoolItem>::create();
    main_actor
        .setup
        .act_on::<StatusReport>(|actor, event| {
            let sender = &event.return_address.sender;

            match event.message {
                StatusReport::Complete(total) => {
                    tracing::info!("{} reported {}", sender.value, total);
                    actor.state.receive_count += total;  // Increment receive_count based on StatusReport
                }
            }
        })
        .on_before_stop(|actor| {
            tracing::info!("Processed {} PONGs", actor.state.receive_count);
        });

    // Set up the pool with a round-robin load balance strategy
    let pool_builder = PoolBuilder::default().add_pool::<PoolItem>("pool", 5, LoadBalanceStrategy::RoundRobin);

    // Activate the main actor with the pool builder
    let main_context = main_actor.activate(Some(pool_builder)).await;

    // Emit PING events to the pool 22 times
    for _ in 0..22 {
        // Uncomment for debugging: tracing::trace!("Emitting PING");
        main_context.emit_pool("pool", Ping).await;
    }

    // Uncomment for debugging: tracing::trace!("Terminating main actor");
    main_context.terminate().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_actor_pool_random() -> anyhow::Result<()> {
    // Initialize tracing for logging purposes
    init_tracing();

    // Create the main actor
    let mut main_actor = Akton::<PoolItem>::create();
    main_actor
        .setup
        .act_on::<StatusReport>(|actor, event| {
            let sender = &event.return_address.sender;
            match event.message {
                StatusReport::Complete(total) => {
                    tracing::debug!("{} reported {}", sender.value, total);
                    actor.state.receive_count += total;  // Increment receive_count based on StatusReport
                }
            }
        })
        .on_before_stop(|actor| {
            tracing::info!("Processed {} PONGs", actor.state.receive_count);
        });

    // Set up the pool with a random load balance strategy
    let pool_builder = PoolBuilder::default().add_pool::<PoolItem>("pool", 5, LoadBalanceStrategy::Random);

    // Activate the main actor with the pool builder
    let main_context = main_actor.activate(Some(pool_builder)).await;

    // Emit PING events to the pool 22 times
    for _ in 0..22 {
        tracing::trace!("Emitting PING");
        main_context.emit_pool("pool", Ping).await;
    }

    tracing::trace!("Terminating main actor");
    main_context.terminate().await?;

    Ok(())
}
