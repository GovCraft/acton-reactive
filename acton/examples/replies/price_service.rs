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

use std::thread::sleep;
use std::time::Duration;

use tracing::*;

use acton::prelude::*;

use crate::CartItem;
use crate::GetPriceRequest;
use crate::GetPriceResponse;
use crate::shopping_cart::ShoppingCart;

// Define the agent's model to get the current price of items.
#[derive(Default, Debug, Clone)]
pub(crate) struct PriceService {
    pub(crate) handle: AgentHandle,
}

impl PriceService {
    pub(crate) async fn new(app: &mut AgentRuntime) -> Self {
        let config = ActorConfig::new(Ern::with_root("price_service").unwrap(),None, None ).expect( "Failed to create actor config");
        let mut price_service = app.create_actor_with_config::<PriceService>(config).await;
        // Configure agent behavior
        price_service
            .act_on::<GetPriceRequest>(|agent, envelope| {
                // Contains the address of the sender, so the agent
                // doesn't need to know about the sender, only that
                // it can reply to the sender, whoever that is.
                let sender = envelope.origin_envelope.clone();
                let item = envelope.message.0.clone();
                let model = agent.model.clone();
                AgentReply::from_async(
                    async move {
                        let response_message = GetPriceResponse {
                            price: model.get_price(item.clone()).await,
                            item,
                        };
                        trace!("Sending response for {} with price: ${}", &response_message.item.name, &response_message.price);
                        sender.send(response_message).await;
                    }
                )
            });

        let handle = price_service.start().await;
        trace!("price_service handle id is {:?}", handle.id());
        PriceService {
            handle,
        }
    }

    // Define a mock async method to get the current price of an item in cents.
    async fn get_price(&self, item: CartItem) -> usize {
        println!("Getting price for {}", item.name);
        tokio::time::sleep(Duration::from_millis(200)).await; // Simulate an async delay, maybe to a database or API
        return 100; // Mock price - everything costs a dollar!
    }
}
