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

use std::any::{Any, TypeId};
use std::fmt::Debug;
use async_trait::async_trait;
use quasar_qrn::prelude::*;
use tokio_util::task::TaskTracker;
use crate::common::{Origin, OutboundSignalChannel};

//region Traits
pub trait QuasarMessage: Any + Sync + Send + Debug {
    fn as_any(&self) -> &dyn Any;
    fn type_id(&self) -> TypeId { TypeId::of::<Self>() }
}

pub trait SystemMessage: Any + Sync + Send + Debug {
    fn as_any(&self) -> &dyn Any;
}

//endregion


#[async_trait]
pub(crate) trait InternalSignalEmitter {
    fn signal_outbox(&mut self) -> &mut OutboundSignalChannel;
    async fn send_lifecycle(&mut self, message: impl SystemMessage) -> anyhow::Result<()> {
        self.signal_outbox().send(Box::new(message)).await?;
        Ok(())
    }
}

#[async_trait]
pub trait ReturnAddress: Send {
    async fn reply(&self, message: impl QuasarMessage) -> anyhow::Result<()>;
}

#[async_trait]
pub trait ActorContext: Sized + Clone {
    fn return_address(&mut self) -> Origin;
    fn get_task_tracker(&mut self) -> &mut TaskTracker;

    fn key(&self) -> &Qrn;

    async fn emit(&mut self, message: impl QuasarMessage) -> anyhow::Result<()> {
        let origin = self.return_address();
        origin.reply(message).await?;
        Ok(())
    }

    /// Immediately stop processing incoming messages and switch to a
    /// `stopping` state. This only affects actors that are currently
    /// `running`. Future attempts to queue messages will fail.
    async fn terminate(self) -> anyhow::Result<()>;
    /// Terminate actor execution unconditionally. This sets the actor
    /// into the `stopped` state. This causes future attempts to queue
    /// messages to fail.

    async fn wake(&mut self) -> anyhow::Result<()>;
    async fn recreate(&mut self) -> anyhow::Result<()>;
    async fn suspend(&mut self) -> anyhow::Result<()>;
    async fn resume(&mut self) -> anyhow::Result<()>;
    async fn supervise(&mut self) -> anyhow::Result<()>;
    async fn watch(&mut self) -> anyhow::Result<()>;
    async fn unwatch(&mut self) -> anyhow::Result<()>;
    async fn failed(&mut self) -> anyhow::Result<()>;
}

