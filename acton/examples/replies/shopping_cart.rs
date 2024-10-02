/*
 * Copyright (c) 2024. Govcraft
 *
 * Licensed under either of
 *   * Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *   * MIT license: http://opensource.org/licenses/MIT
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the applicable License for the specific language governing permissions and
 * limitations under that License.
 */
use std::collections::HashMap;
use std::ops::AddAssign;

use mti::prelude::MagicTypeId;
use tracing::*;
use tracing::field::debug;

use acton::prelude::*;

use crate::{FinalizeSale, GetPriceRequest, PrinterMessage};
use crate::cart_item::{CartItem, Price};
use crate::ItemScanned;
use crate::price_service::PriceService;
use crate::PriceResponse;

// Define the agent's model to track a list of items.
// This demonstrates that no locks or atomics are needed.
#[derive(Default, Debug, Clone)]
pub(crate) struct ShoppingCart {
    items: HashMap<MagicTypeId, CartItem>,
    subtotal: i32,
    pub(crate) price_service: AgentHandle,
}


impl ShoppingCart {
    pub(crate) async fn new(app: &mut AgentRuntime) -> anyhow::Result<AgentHandle> {
        let shopping_cart = app.spawn_actor::<ShoppingCart>(|mut agent| {
            Box::pin(async move {
                agent
                    .act_on::<ItemScanned>(|agent, context| {
                        let price_service = &agent.model.price_service;

                        let item = context.message().0.clone();
                        trace!("AddItem: {}", &item.name());
                        agent.model.items.insert(item.id().clone(), item.clone());
                        debug_assert!(!agent.model.items.is_empty(), "Shopping cart is empty");

                        trace!("Sending to price service with name: {}", price_service.name());
                        let envelope = context.new_envelope(&price_service.reply_address());

                        AgentReply::from_async(async move {
                            trace!( "Sending GetPriceRequest for item: {}", item.name());
                            envelope.send(GetPriceRequest(item)).await;
                        })
                    })
                    .before_stop(|agent| {
                        // display the total price of the shopping cart
                        trace!("Subtotal: {}", agent.model.subtotal);
                        let broker = agent.broker().clone();
                        let subtotal = Price(agent.model.subtotal);
                        Box::pin(async move {
                            broker.broadcast(FinalizeSale(subtotal)).await;
                        })
                    });

                let price_service = agent.handle().supervise::<PriceService>(PriceService::new(&mut agent.runtime().clone()).await).await.expect("TODO: panic message");

                agent.model.price_service = price_service;

                //subscribe to price service messages
                trace!("Subscribing to GetPriceResponse");
                agent.start().await
            })
        }).await?;

        Ok(shopping_cart)
    }
}