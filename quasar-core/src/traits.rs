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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use async_trait::async_trait;
use quasar_qrn::prelude::*;
use tokio::sync::Mutex;
use tokio_util::task::TaskTracker;
use crate::common::{ActorPool, Awake, Context, ContextPool, EventRecord, MessageError, OutboundEnvelope};

#[async_trait]
pub trait Handler {
    async fn handle(&mut self) -> Result<(), MessageError>;
}

// pub trait IntoAsyncReactor<T, U> {
//     fn into_reactor(self) -> Box<dyn Fn(Arc<Mutex<Awake<T, U>>>, &EventRecord<U>) -> Pin<Box<dyn Future<Output=()> + Send>> + Send + Sync>;
// }
//
// impl<T, U, F, Fut> IntoAsyncReactor<T, U> for F
//     where
//         T: 'static + Send + Sync,
//         U: QuasarMessage + 'static + Clone,
//         F: Fn(Arc<Mutex<Awake<T, U>>>, &EventRecord<U>) -> Fut + Send + Sync + 'static,
//         Fut: Future<Output = ()> + Send + 'static,
// {
//     fn into_reactor(self) -> Box<dyn Fn(Arc<Mutex<Awake<T, U>>>, &EventRecord<U>) -> Pin<Box<dyn Future<Output=()> + Send>> + Send + Sync> {
//         Box::new(move |actor, event| {
//             Box::pin(self(actor, event))
//         })
//     }
// }


//region Traits
pub trait QuasarMessage: Any + Send + Debug {
    fn as_any(&self) -> &dyn Any;
    fn type_id(&self) -> TypeId { TypeId::of::<Self>() }
}

pub trait SystemMessage: Any + Send + Sync + Debug {
    fn as_any(&self) -> &dyn Any;
}

//endregion


// #[async_trait]
// pub(crate) trait InternalSignalEmitter {
//     fn signal_outbox(&mut self) -> &mut OutboundSignalChannel;
//     async fn send_lifecycle(&mut self, message: impl SystemMessage + Send + 'static) -> Result<(),MessageError> {
//         self.signal_outbox().send(Box::new(message)).await?;
//         Ok(())
//     }
// }

#[async_trait]
pub trait ReturnAddress: Send {
    async fn reply(&self, message: Box<dyn QuasarMessage>) -> Result<(), MessageError>;
}

#[async_trait]
pub trait ConfigurableActor: Default + Send + Debug {
    async fn init(name: String, root: &Context) -> Context;
}

pub enum DistributionStrategy {
    RoundRobin,
    Random,
    LeastBusy,
    HashBased,
}

#[async_trait]
pub trait ActorContext {
    fn return_address(&self) -> OutboundEnvelope;
    fn get_task_tracker(&mut self) -> &mut TaskTracker;

    // fn get_pool(&self, name: &str) -> Option<ContextPool>;

    fn key(&self) -> &Qrn;

    fn emit(&self, message: impl QuasarMessage + Sync + Send + 'static) -> impl Future<Output=Result<(), MessageError>> + Sync where Self: Sync {
        async {
            let envelope = self.return_address();
            envelope.reply(message).await?; // Directly boxing the owned message
            Ok(())
        }
    }

    fn pool_emit<DistributionStrategy>(&self, index: usize, name: &str, message: impl QuasarMessage + Sync + Send + 'static) -> impl Future<Output=Result<(), MessageError>> + Sync;
    fn terminate(&self) -> impl Future<Output=Result<(), MessageError>> + Sync;
    async fn spawn_pool<T: ConfigurableActor + 'static>(&mut self, name: &str, size: usize) -> anyhow::Result<()>;
    async fn wake(&mut self) -> anyhow::Result<()>;
    async fn recreate(&mut self) -> anyhow::Result<()>;
    async fn suspend(&mut self) -> anyhow::Result<()>;
    async fn resume(&mut self) -> anyhow::Result<()>;
    async fn supervise(&mut self) -> anyhow::Result<()>;
    async fn watch(&mut self) -> anyhow::Result<()>;
    async fn unwatch(&mut self) -> anyhow::Result<()>;
    async fn failed(&mut self) -> anyhow::Result<()>;
}



