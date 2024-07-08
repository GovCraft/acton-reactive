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
use std::any::TypeId;
use crate::setup::*;
use akton::prelude::*;
use tracing::*;
use akton_test::prelude::*;

#[akton_test]
async fn test_messaging_behavior() -> anyhow::Result<()> {
    initialize_tracing();
    let mut akton: AktonReady = Akton::launch().into();
    let mut actor = akton.create_actor::<PoolItem>().await;
    actor

        .act_on::<Ping>(|actor, event| {
            let message = event.message.clone();
            let type_id = TypeId::of::<Ping>();
            let type_name = std::any::type_name::<Ping>();
            info!(type_name=type_name,type_id=?type_id, "Received");
            actor.entity.receive_count += 1;
        })
        .before_stop(|actor| {
            info!("Processed {} Pings", actor.entity.receive_count);
        });
    let actor_ref = actor.activate().await;
    actor_ref.emit(Ping).await;
    actor_ref.suspend().await?;
    Ok(())
}

#[akton_test]
async fn test_async_messaging_behavior() -> anyhow::Result<()> {
    initialize_tracing();
    let mut akton: AktonReady = Akton::launch().into();
    let mut actor = akton.create_actor::<PoolItem>().await;
    actor

        .act_on_async::<Ping>(|actor, event| {
            let message = event.message.clone();
            let type_id = TypeId::of::<Ping>();
            let type_name = std::any::type_name::<Ping>();
            info!(type_name=type_name,type_id=?type_id, "Received in async handler");
            actor.entity.receive_count += 1;
            Box::pin(async move {})
        })
        .before_stop(|actor| {
            info!("Processed {} Pings", actor.entity.receive_count);
        });
    let context = actor.activate().await;
    context.emit(Ping).await;
    context.suspend().await?;
    Ok(())
}
