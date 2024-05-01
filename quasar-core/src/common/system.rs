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

use std::fmt::Debug;
use quasar_qrn::Qrn;
use crate::common::{Actor, Idle};

#[derive(Debug)]
pub struct System<State: Default + Send + Debug> {
    pub root_actor: State
}

impl<State: Default + Send + Debug> System<State> {
    pub fn new_actor(actor: State) -> Actor<Idle<State>, State>
    where State: Default{

        //append to the qrn

        Actor::new(Qrn::default(), actor, None)
    }
}

impl<State: Default + Send + Debug> Default for System<State> {
    fn default() -> Self {
        System{ root_actor: State::default() }
    }
}