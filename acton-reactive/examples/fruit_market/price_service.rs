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

// Price Service: Simulates looking up prices for items
//
// This service acts like a price database or barcode scanner:
// - Receives requests for item prices
// - Simulates a brief delay (like a real lookup would have)
// - Returns random prices (for demonstration purposes)
// - Broadcasts price information to other agents

use acton_reactive::prelude::*;
use rand::Rng;
use std::time::Duration;
use tracing::*;

use crate::PriceResponse;
use crate::{CartItem, ItemScanned};

// Constants for our service
const PRICE_SERVICE_ROOT: &str = "price_service"; // Service identifier
const MOCK_DELAY_MS: u64 = 100; // Simulated lookup time
const PRICE_MIN: i32 = 100; // Minimum price ($1.00)
const PRICE_MAX: i32 = 250; // Maximum price ($2.50)

// Our price lookup service
#[derive(Default, Debug, Clone)]
pub(crate) struct PriceService;

impl PriceService {
    // Create a new price service agent
    #[instrument(skip(app))]
    pub(crate) async fn new(app: &mut AgentRuntime) -> anyhow::Result<AgentHandle> {
        // Set up the service configuration
        let config = AgentConfig::new(Ern::with_root(PRICE_SERVICE_ROOT).unwrap(), None, None)?;

        // Create our price service agent
        let mut price_service = app.create_actor_with_config::<PriceService>(config).await;

        // Tell the service how to handle item scan messages
        price_service.act_on::<ItemScanned>(|agent, context| {
            let item = context.message().0.clone();
            let model = agent.model.clone();
            let broker = agent.broker().clone();

            // Look up the price and broadcast the result
            AgentReply::from_async(async move {
                let mut item = item;
                // Get a price for this item
                item.set_cost(model.get_price(item.clone()).await);

                // Create and broadcast our response
                let response_message = PriceResponse { item };
                broker.broadcast(response_message).await;
            })
        });

        Ok(price_service.start().await)
    }

    // Simulate looking up a price for an item
    async fn get_price(&self, item: CartItem) -> i32 {
        trace!("Getting price for {}", item.name());

        // Simulate the delay of a real price lookup
        tokio::time::sleep(Duration::from_millis(MOCK_DELAY_MS)).await;

        // Generate a random price between PRICE_MIN and PRICE_MAX
        // (In a real system, this would look up actual prices from a database)
        rand::thread_rng().gen_range(PRICE_MIN..=PRICE_MAX)
    }
}
