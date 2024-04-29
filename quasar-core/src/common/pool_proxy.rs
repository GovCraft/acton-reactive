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

use std::any::Any;
use async_trait::async_trait;
use crate::common::{Context, ContextPool, System};
use crate::traits::{ConfigurableActor, QuasarMessage};

#[derive(Debug)]
pub struct NewPoolMessage {
    name: &'static str,
}

impl<'a> QuasarMessage for NewPoolMessage {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Default, Debug)]
pub struct PoolProxy {
    pub pool: ContextPool,
}

#[async_trait]
impl ConfigurableActor for PoolProxy {
    async fn init(name: &str) -> Context {
        let proxy = System::new(PoolProxy::default());

        proxy.spawn().await
    }
}