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
use crate::common::{Awake, EventRecord, Origin, OutboundSignalChannel};

#[async_trait]
pub trait Handler {
    async fn handle(&mut self) -> anyhow::Result<()>;
}

pub trait IntoAsyncReactor<T, U> {
    fn into_reactor(self) -> Box<dyn Fn(Arc<Mutex<Awake<T, U>>>, &EventRecord<U>) -> Pin<Box<dyn Future<Output=()> + Send>> + Send + Sync>;
}

impl<T, U, F, Fut> IntoAsyncReactor<T, U> for F
    where
        T: 'static + Send + Sync,
        U: QuasarMessage + 'static + Clone,
        F: Fn(Arc<Mutex<Awake<T, U>>>, &EventRecord<U>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
{
    fn into_reactor(self) -> Box<dyn Fn(Arc<Mutex<Awake<T, U>>>, &EventRecord<U>) -> Pin<Box<dyn Future<Output=()> + Send>> + Send + Sync> {
        Box::new(move |actor, event| {
            Box::pin(self(actor, event))
        })
    }
}


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
    async fn reply(&self, message: Box<dyn QuasarMessage>) -> anyhow::Result<()>;
}

#[async_trait]
pub trait ActorContext<M: QuasarMessage + Send + 'static + ?Sized>: Sized + Clone {
    fn return_address(&mut self) -> Origin;
    fn get_task_tracker(&mut self) -> &mut TaskTracker;

    fn key(&self) -> &Qrn;

    async fn emit(&mut self, message: impl QuasarMessage + Send + 'static) -> anyhow::Result<()> {
        let origin = self.return_address();
        origin.reply(Box::new(message)).await?; // Directly boxing the owned message
        Ok(())
    }
    async fn terminate(self) -> anyhow::Result<()>;
    async fn wake(&mut self) -> anyhow::Result<()>;
    async fn recreate(&mut self) -> anyhow::Result<()>;
    async fn suspend(&mut self) -> anyhow::Result<()>;
    async fn resume(&mut self) -> anyhow::Result<()>;
    async fn supervise(&mut self) -> anyhow::Result<()>;
    async fn watch(&mut self) -> anyhow::Result<()>;
    async fn unwatch(&mut self) -> anyhow::Result<()>;
    async fn failed(&mut self) -> anyhow::Result<()>;
}





// pub trait AsyncMessageReactor<T, U>:Send {
//     fn call(&self, actor: &mut Awake<T, U>, envelope: &Envelope) -> Pin<Box<dyn Future<Output = ()> + Send>>;
// }
//
//
// impl<T, U, F> AsyncMessageReactor<T, U> for F
//     where
//         T: Send + 'static,
//         U: Send + 'static,
//         F: Fn(&mut Awake<T, U>, &Envelope) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static,
// {
//     fn call(&self, actor: &mut Awake<T, U>, envelope: &Envelope) -> Pin<Box<dyn Future<Output = ()> + Send>>{
//         (self)(actor, envelope)
//     }
// }

