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

use std::pin::Pin;
use std::task::{Context, Poll};
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;
use crate::common::{Awake, Context as ActorContext, EventRecord};
use crate::traits::QuasarMessage;  // If using async Mutex from Tokio

pub struct MessageProcessingFuture<T: 'static, U: 'static, M, Fut> {
    pub(crate) inner: Fut,
    pub(crate) actor: Arc<AsyncMutex<Awake<T, U>>>,
    pub(crate) event: EventRecord<M>,
}

impl<T, U, M, Fut> Future for MessageProcessingFuture<T, U, M, Fut>
    where
        T: Send,
        M: QuasarMessage + Unpin,
        Fut: Future<Output=()> + Unpin,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Use Pin::new to safely get a mutable reference to the inner future
        let fut = Pin::new(&mut self.inner);
        fut.poll(cx)
    }
}