/*
 *
 *  *
 *  * Copyright (c) 2024 Govcraft.
 *  *
 *  *  Licensed under the Business Source License, Version 1.1 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *      https://github.com/GovCraft/acton-framework/tree/main/LICENSES
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

// We'll create pool of audience member actor who will hear a joke told by the comedian
// They will randomly react to the jokes after which the Comedian will report on how many
// jokes landed and didn't land

use std::future::Future;
use std::pin::Pin;

use async_trait::async_trait;
use rand::Rng;
use tracing::{debug, error, info, trace};

use acton_core::prelude::*;
use acton_macro::acton_actor;

use crate::setup::*;

#[acton_actor]
pub struct AudienceMember {
    pub jokes_told: usize,
    pub funny: usize,
    pub bombers: usize,
}

impl AudienceMember {
    // This trait function details what should happen for each member of the pool we are about to
    // create, it gets created when the parent actor calls spawn_with_pool
    async fn initialize(&self, config: ActorConfig) -> ActorRef {
        let mut acton: SystemReady = Acton::launch().into();

        let broker = acton.get_broker();

        let actor_config = ActorConfig::new(
            Ern::with_root("improve_show").expect("Couldn't create pool member Ern"),
            None,
            Some(broker.clone()),
        );

        let mut actor = acton.act_on::<AudienceMember>().await; //::<Comedian>::create_with_config(actor_config);
        // let mut actor =
        //     Akton::<AudienceMember>::create_with_config(config.clone());

        actor.act_on_async::<Joke>(|actor, event| {
            let sender = actor.new_parent_envelope().unwrap();
            // let parent_sender = actor.new_parent_envelope().sender.value;
            // let event_sender = &event.return_address.sender.value;
            let mut rng = rand::thread_rng();
            let random_reaction = rng.gen_bool(0.5);

            let reaction = {
                if random_reaction {
                    AudienceReactionMsg::Chuckle
                } else {
                    AudienceReactionMsg::Groan
                }
            };
            Box::pin(async move {
                sender.reply_async(reaction).await
            })
        });

        // Event: Activating AudienceMember
        // Description: Activating the AudienceMember actor.
        // Context: None
        trace!("Activating the AudienceMember actor.");
        actor
            .activate().await
    }
}
