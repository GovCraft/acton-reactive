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
use tracing::*;

use acton::prelude::*;

use crate::AddItem;
use crate::cart_item::CartItem;
use crate::GetPriceRequest;
use crate::GetPriceResponse;

// Define the agent's model to track a list of items.
// This demonstrates that no locks or atomics are needed.
#[derive(Default, Debug, Clone)]
pub(crate) struct ShoppingCart {
    items: Vec<CartItem>,
    agent_handle: AgentHandle,
    pub(crate) price_service_handle: AgentHandle,
}

impl ShoppingCart {
    pub(crate) async fn new(price_service_handle: AgentHandle, app: &mut AgentRuntime) -> Self {
        // let mut shopping_cart = app.initialize::<ShoppingCart>().await;
        let config = ActorConfig::new(Ern::with_root("shopping_cart").unwrap(), None, None).expect("Failed to create actor config");
        let mut shopping_cart = app.create_actor_with_config::<ShoppingCart>(config).await;
        // Configure agent behavior
        shopping_cart
            .act_on::<AddItem>(|agent, envelope| {
                let item = envelope.message.0.clone();
                info!("Adding item: {}", &item.name);
                agent.model.items.push(item.clone());
                // agent.model.price_service_handle.id();
                let price_service = agent.model.price_service_handle.clone();
                trace!( "Sending to price_service id: {:?}", agent.model.price_service_handle.clone());
                AgentReply::from_async(async move {
                    let envelope = price_service.create_envelope(None);
                    trace!( "Sending GetPriceRequest for item: {}", item.name);
                    envelope.send(GetPriceRequest(item)).await;
                    // Send a message to the price service to get the price of the item.
                    // price_service.send_message(GetPriceRequest(item)).await;
                })
            })
            .act_on::<GetPriceResponse>(|agent, envelope| {
                info!("Updating cart item with price: {}", envelope.message.price);
                // agent.model.items.push(envelope.message.0.clone());
                AgentReply::immediate()
            });

        let agent_handle = shopping_cart.start().await;

        trace!( "Shopping cart actor created with price_service id: {}", price_service_handle.id());
        trace!("shopping_cart handle id is {:?}", agent_handle.id());
        ShoppingCart {
            items: Vec::new(),
            agent_handle,
            price_service_handle,
        }
    }
    pub(crate) async fn add_item(&self, name: impl Into<String>, quantity: usize) {
        //note: no need for &mut self because the request is sent to the agent as a message
        let name = name.into();
        trace!("agent_handle id is {:?}", self.agent_handle.id());
        trace!("and pricing_service id is {:?}", self.price_service_handle.id());
        self.agent_handle.send_message(AddItem(CartItem { name, quantity, price: 0 })).await;
    }
    // Define a method to get the total price of all items in the cart.
    pub(crate) fn total_price(&self) -> usize {
        self.items.iter().map(|item| item.price * item.quantity).sum()
    }
}