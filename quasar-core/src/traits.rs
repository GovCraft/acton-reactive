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
use crate::common::{ActorPool, Awake, Context, ContextPool, EventRecord, OutboundEnvelope, OutboundSignalChannel};

#[async_trait]
pub trait Handler {
    async fn handle(&mut self) -> anyhow::Result<()>;
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
pub trait ConfigurableActor: Default + Send + Sync + Debug {
    async fn init(name: &str) -> Context;
}
pub enum DistributionStrategy {
    RoundRobin,
    Random,
    LeastBusy,
    HashBased
}
#[async_trait]
pub trait ActorContext {
    fn return_address(&self) -> OutboundEnvelope;
    fn get_task_tracker(&mut self) -> &mut TaskTracker;

    // fn get_pool(&self, name: &str) -> Option<ContextPool>;

    fn key(&self) -> &Qrn;

    async fn emit(&self, message: impl QuasarMessage + Send + 'static) -> anyhow::Result<()> {
        let envelope = self.return_address();
        envelope.reply(message).await?; // Directly boxing the owned message
        Ok(())
    }

    async fn pool_emit<DistributionStrategy>(&self, name:&str, message: impl QuasarMessage + Send + 'static) -> anyhow::Result<()> {
        //get context pool by name
       // if let Some(context_pool) = self.get_pool(name) {
               //find the next context in the vector to send to based on it's key and it's round-robin position
                //the key was generated like this:
               // async fn spawn_pool<T: ConfigurableActor + 'static>(&mut self, name: &str, size: usize) -> anyhow::Result<()> {
               //     let mut pool = Vec::with_capacity(size);
               //     for i in 0..size {
               //         let actor_name = format!("{}{}", name, i);
               //         let context = T::init(&actor_name).await;
               //         pool.push(context);
               //     }
               //     self.pools.insert(name.to_string(), pool);
               //     Ok(())
               // }

           //create a distribution proxy actor to hold the pool of Contexts and maintain state required
           //to pass messages based on the specified distribution strategy
           //it will be up to that actor to emit to the contexts it proxies for
           //so the context of this actor will not hold a pool of pools, it will hold a pool of distribution proxy contexts

                //then call emit on the context with the message passed into this function

       // }
        Ok(())
    }
    async fn terminate(self) -> anyhow::Result<()>;
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

