/*
 * Copyright (c) 2024. Govcraft
 *
 * Licensed under either of
 *   * Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *   * MIT license: http://opensource.org/licenses/MIT
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the applicable License for the specific language governing permissions and
 * limitations under that License.
 */


use acton_ern::{Ern, ErnParser, UnixTime};

use crate::common::{BrokerRef, ParentRef};
use crate::traits::Actor;

#[derive(Default, Debug, Clone)]
pub struct ActorConfig {
    ern: Ern<UnixTime>,
    broker: Option<BrokerRef>,
    parent: Option<ParentRef>,
}

impl ActorConfig {
    pub fn new(
        ern: Ern<UnixTime>,
        parent: Option<ParentRef>,
        broker: Option<BrokerRef>,
    ) -> anyhow::Result<ActorConfig> {
        if let Some(parent) = parent {
            //get the parent arn
            let parent_ern = ErnParser::new(parent.ern().to_string()).parse()?;
            let child_ern = parent_ern + ern;
            Ok(ActorConfig {
                ern: child_ern,
                broker,
                parent: Some(parent),
            })
        } else {
            Ok(ActorConfig {
                ern,
                broker,
                parent,
            })
        }
    }
    pub(crate) fn ern(&self) -> Ern<UnixTime> {
        self.ern.clone()
    }
    pub(crate) fn get_broker(&self) -> &Option<BrokerRef> {
        &self.broker
    }
    pub(crate) fn parent(&self) -> &Option<ParentRef> {
        &self.parent
    }
}
