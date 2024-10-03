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

/// Configuration for creating an actor.
///
/// This struct holds the necessary information to configure an actor,
/// including its ERN (Entity Resource Name), broker, and parent reference.
#[derive(Default, Debug, Clone)]
pub struct AgentConfig {
    ern: Ern<UnixTime>,
    pub(crate) broker: Option<BrokerRef>,
    parent: Option<ParentRef>,
}

impl AgentConfig {
    /// Creates a new `ActorConfig` instance.
    ///
    /// # Arguments
    ///
    /// * `ern` - The Entity Resource Name for the actor.
    /// * `parent` - An optional parent reference.
    /// * `broker` - An optional broker reference.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the new `ActorConfig` instance or an error.
    pub fn new(
        ern: Ern<UnixTime>,
        parent: Option<ParentRef>,
        broker: Option<BrokerRef>,
    ) -> anyhow::Result<AgentConfig> {
        if let Some(parent) = parent {
            // Get the parent ERN
            let parent_ern = ErnParser::new(parent.id().to_string()).parse()?;
            let child_ern = parent_ern + ern;
            Ok(AgentConfig {
                ern: child_ern,
                broker,
                parent: Some(parent),
            })
        } else {
            Ok(AgentConfig {
                ern,
                broker,
                parent,
            })
        }
    }

    /// Creates a new config with an ERN root with the provided name.
    pub fn new_with_name(
        name: impl Into<String>,
    ) -> anyhow::Result<AgentConfig> {
        Self::new(Ern::with_root(name.into())?, None, None)
    }


    /// Returns the ERN of the actor.
    pub(crate) fn ern(&self) -> Ern<UnixTime> {
        self.ern.clone()
    }

    /// Returns a reference to the optional broker.
    pub(crate) fn get_broker(&self) -> &Option<BrokerRef> {
        &self.broker
    }

    /// Returns a reference to the optional parent.
    pub(crate) fn parent(&self) -> &Option<ParentRef> {
        &self.parent
    }
}
