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

// We'll create pool of audience member actors who will hear a joke told by the comedian
// They will randomly react to the jokes after which the Comedian will report on how many
// jokes landed and didn't land

use rand::Rng;
use async_trait::async_trait;
use akton_core::prelude::*;
use crate::setup::*;

#[derive(Default, Debug, Clone)]
pub struct AudienceMember {
    pub jokes_told: usize,
    pub funny: usize,
    pub bombers: usize,
}

#[async_trait]
impl PooledActor for AudienceMember {
    // this trait function details what should happen for each member of the pool we are about to
    // create, it gets created when the parent actor calls spawn_with_pool
    async fn initialize(&self, name: String, root: &Context) -> Context {
        let mut parent = root.supervise::<AudienceMember>(&name);
        parent.setup.act_on::<Joke>(|actor, _event| {
            let sender = &actor.new_parent_envelope();
            let mut random_choice = rand::thread_rng();
            let random_reaction = random_choice.gen_bool(0.5);
            if random_reaction {
                tracing::trace!("Send chuckle");
                let _ = sender.reply(AudienceReactionMsg::Chuckle, Some("audience".to_string()));
            } else {
                tracing::trace!("Send groan");
                let _ = sender.reply(AudienceReactionMsg::Groan, Some("audience".to_string()));
            }
        });
        let context = parent.activate(None).await;
        context
    }
}
