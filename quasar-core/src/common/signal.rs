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

use crate::traits::QuasarMessage;
use std::{any::Any, fmt::Debug};
use tokio::sync::oneshot::Sender;
#[derive(Debug)]
#[non_exhaustive]
pub enum SupervisorSignal<T: Any + Send + Debug> {
    Inspect(Option<Sender<T>>),
}
impl<T: Any + Send + Debug> QuasarMessage for SupervisorSignal<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        // Implement the new method
        self
    }
}
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SystemSignal {
    Wake,
    Recreate,
    Suspend,
    Resume,
    Terminate,
    Supervise,
    Watch,
    Unwatch,
    Failed,
}
impl QuasarMessage for SystemSignal {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        // Implement the new method
        self
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
