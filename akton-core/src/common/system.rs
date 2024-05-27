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

use tracing::instrument;

use crate::common::{Actor, Idle};
use std::fmt::Debug;

#[derive(Debug)]
pub struct System<State: Clone + Default + Send + Debug> {
    pub root_actor: State,
}

impl<State: Clone + Default + Send + Debug> System<State> {
    #[instrument]
    pub fn new_actor() -> Actor<Idle<State>, State>
    where
        State: Default,
    {
        //append to the qrn

        Actor::new("root", State::default(), None)
    }
}

impl<State: Clone + Default + Send + Debug> Default for System<State> {
    fn default() -> Self {
        System {
            root_actor: State::default(),
        }
    }
}
