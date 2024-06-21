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
use akton::prelude::*;
use tracing::*;
use crate::setup::*;

mod setup;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_broker() -> anyhow::Result<()> {
    init_tracing();

    let broker = Akton::<Broker>::spawn_broker().await?;

    let actor_config = ActorConfig::new(
        "improve_show",
        None,
        Some(broker.clone()),
    );

    let mut comedy_show = Akton::<Comedian>::create_with_config(actor_config);


    comedy_show
        .setup
        .act_on_async::<Ping>(|actor, event| {

            info!("SUCCESS! PING!");
            Box::pin(async move {  })
        });


    let comedian = comedy_show.activate(None).await?;
    comedian.subscribe::<Ping>().await;
    let broadcast_message = BrokerRequest::new(Ping);

    broker.emit_async(broadcast_message, None).await;

    let _ = comedian.suspend().await?;
    let _ = broker.suspend().await?;

    Ok(())
}

