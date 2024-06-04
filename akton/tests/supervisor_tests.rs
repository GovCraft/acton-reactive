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
async fn test_audience_pool() -> anyhow::Result<()> {
    init_tracing();
    let mut audience = Akton::<AudienceMember>::create();

    audience
        .setup
        .act_on::<AudienceReactionMsg>(|actor, event| {
            match event.message {
                AudienceReactionMsg::Groan => actor.state.funny += 1,
                AudienceReactionMsg::Chuckle => actor.state.bombers += 1,
            }
            actor.state.jokes_told += 1;
        })
        .on_before_stop(|actor| {
            tracing::info!(
                "Jokes Told: {}\tFunny: {}\tBombers: {}",
                actor.state.jokes_told,
                actor.state.funny,
                actor.state.bombers
            );
        });
    let pool =
        PoolBuilder::default().add_pool::<AudienceMember>("audience", 10, LoadBalanceStrategy::Random);

    //here we call the init method on the 1000 pool members we created
    let context = audience.activate(Some(pool)).await?;

    for _ in 0..10 {
        context.emit_pool("audience", Joke).await;
    }
    context.terminate().await?;
    Ok(())
}

