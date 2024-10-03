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

use std::time::Duration;

use rand::Rng;
use tracing::*;

use acton::prelude::*;

use crate::{CartItem, ItemScanned};
use crate::PriceResponse;

const PRICE_SERVICE_ROOT: &str = "price_service";
const MOCK_DELAY_MS: u64 = 100;
const PRICE_MIN: i32 = 100;
const PRICE_MAX: i32 = 250;

#[derive(Default, Debug, Clone)]
pub(crate) struct PriceService;

impl PriceService {
    #[instrument(skip(app))]
    pub(crate) async fn new(app: &mut AgentRuntime) -> anyhow::Result<AgentHandle> {
        let config =
            AgentConfig::new(Ern::with_root(PRICE_SERVICE_ROOT).unwrap(), None, None)?;
        let mut price_service = app.create_actor_with_config::<PriceService>(config).await;

        price_service.act_on::<ItemScanned>(|agent, context| {
            let item = context.message().0.clone();
            let model = agent.model.clone();
            let broker = agent.broker().clone();

            AgentReply::from_async(async move {
                let mut item = item;
                item.set_cost(model.get_price(item.clone()).await);
                let response_message = PriceResponse { item };
                broker.broadcast(response_message).await;
            })
        });

        Ok(price_service.start().await)
    }

    async fn get_price(&self, item: CartItem) -> i32 {
        trace!("Getting price for {}", item.name());
        tokio::time::sleep(Duration::from_millis(MOCK_DELAY_MS)).await;
        rand::thread_rng().gen_range(PRICE_MIN..=PRICE_MAX)
    }
}
