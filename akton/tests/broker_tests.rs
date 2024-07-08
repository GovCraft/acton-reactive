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
use std::sync::Arc;
use akton::prelude::*;
use tracing::*;
use crate::setup::*;

mod setup;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_broker() -> anyhow::Result<()> {
    initialize_tracing();
let mut akton:AktonReady = Superpos::launch().into();
    let broker = akton.get_broker();

    let actor_config = ActorConfig::new(
        Arn::with_root("improve_show").unwrap(),
        None,
        Some(broker.clone()),
    )?;

    let mut comedy_show = akton.create_with_config::<Comedian>(actor_config);

    let actor_config = ActorConfig::new(
        Arn::with_root("counter").unwrap(),
        None,
        Some(broker.clone()),
    )?;
    let mut counter_actor = akton.create_with_config::<Counter>(actor_config);
    counter_actor

        .act_on::<Pong>(|actor, event| {
            info!("Also SUCCESS! PONG!");
        });

    comedy_show

        .act_on_async::<Ping>(|actor, event| {
            info!("SUCCESS! PING!");
            Box::pin(async move {})
        })
        .act_on::<Pong>(|actor, event| {
        info!("SUCCESS! PONG!");
    });

    counter_actor.context.subscribe::<Pong>().await;
    comedy_show.context.subscribe::<Ping>().await;
    comedy_show.context.subscribe::<Pong>().await;

    let comedian = comedy_show.activate(None);
    let counter = counter_actor.activate(None);

    broker.emit(BrokerRequest::new(Ping)).await;
    broker.emit(BrokerRequest::new(Pong)).await;

    let _ = comedian.suspend().await?;
    let _ = counter.suspend().await?;
    let _ = broker.suspend().await?;

    Ok(())
}

