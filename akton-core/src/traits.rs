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

use crate::common::{Context, MessageError, OutboundEnvelope};
use crate::prelude::Envelope;
use async_trait::async_trait;
use akton_arn::prelude::*;
use std::any::{Any, TypeId};
use std::fmt::Debug;
use std::future::Future;
use std::usize;
use tokio_util::task::TaskTracker;
use tracing::instrument;

pub(crate) trait LoadBalancerStrategy: Send + Sync + Debug {
    fn select_item(&mut self, items: &[Context]) -> Option<usize>;
}

#[async_trait]
pub trait Handler {
    async fn handle(&mut self) -> Result<(), MessageError>;
}

pub trait AktonMessage: Any + Send + Debug {
    fn as_any(&self) -> &dyn Any;
    fn type_id(&self) -> TypeId {
        TypeId::of::<Self>()
    }
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

pub trait SystemMessage: Any + Send + Sync + Debug {
    fn as_any(&self) -> &dyn Any;
}

#[async_trait]
pub trait ReturnAddress: Send {
    async fn reply(&self, message: Box<dyn AktonMessage>) -> Result<(), MessageError>;
}

#[async_trait]
pub trait ConfigurableActor: Send + Debug {
    async fn init(&self, name: String, root: &Context) -> Context;
}

pub enum DistributionStrategy {
    RoundRobin,
    Random,
    LeastBusy,
    HashBased,
}

#[async_trait]
pub(crate) trait SupervisorContext: ActorContext {
    fn supervisor_task_tracker(&self) -> TaskTracker;
    fn supervisor_return_address(&self) -> Option<OutboundEnvelope>;

    #[instrument(skip(self))]
    fn emit_envelope(
        &self,
        envelope: Envelope,
    ) -> impl Future<Output = Result<(), MessageError>> + Sync
    where
        Self: Sync,
    {
        async {
            let forward_address = self.return_address();
            if let Some(reply_to) = forward_address.reply_to {
                reply_to.send(envelope).await?;
            }
            Ok(())
        }
    }

    fn pool_emit(
        &self,
        name: &str,
        message: impl AktonMessage + Sync + Send + 'static,
    ) -> impl Future<Output = Result<(), MessageError>> + Sync
    where
        Self: Sync,
    {
        async {
            if let Some(envelope) = self.supervisor_return_address() {
                envelope.reply(message, Some(name.to_string()))?; // Directly boxing the owned message
            }
            Ok(())
        }
    }
}
#[async_trait]
pub trait ActorContext {
    fn return_address(&self) -> OutboundEnvelope;
    fn get_task_tracker(&self) -> TaskTracker;

    fn key(&self) -> &Arn;

    #[instrument(skip(self))]
    fn emit(
        &self,
        message: impl AktonMessage + Sync + Send + 'static,
    ) -> impl Future<Output = Result<(), MessageError>> + Sync
    where
        Self: Sync,
    {
        async {
            let envelope = self.return_address();
            envelope.reply(message, None)?; // Directly boxing the owned message
            Ok(())
        }
    }

    async fn wake(&mut self) -> anyhow::Result<()>;
    async fn recreate(&mut self) -> anyhow::Result<()>;
    async fn suspend(&mut self) -> anyhow::Result<()>;
    async fn resume(&mut self) -> anyhow::Result<()>;
    async fn supervise(&mut self) -> anyhow::Result<()>;
    async fn watch(&mut self) -> anyhow::Result<()>;
    async fn unwatch(&mut self) -> anyhow::Result<()>;
    async fn failed(&mut self) -> anyhow::Result<()>;
}
