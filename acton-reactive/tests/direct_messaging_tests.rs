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

use tracing::{debug, info, instrument, trace};

use acton_reactive::prelude::*;
use acton_test::prelude::*;

use crate::setup::*;

mod setup;

#[derive(Default, Debug, Clone)]
pub(crate) struct ShoppingCart {
    agent_handle: AgentHandle,
    pub(crate) price_service_handle: AgentHandle,
}


#[acton_test]
async fn test_reply() -> anyhow::Result<()> {
    initialize_tracing();
    // Set up the system and create agents
    let mut app = ActonApp::launch();

    let price_service = PriceService::new(&mut app).await;
    let price_service_handle = price_service.agent_handle.clone();
    let shopping_cart = ShoppingCart::new(price_service_handle, &mut app).await;

    shopping_cart.trigger().await;
    shopping_cart.trigger().await;
    shopping_cart.trigger().await;


    // Shut down the system and all agents
    app.shutdown_all().await.expect("Failed to shut down system");
    Ok(())
}

#[derive(Default, Debug, Clone)]
pub struct PongResponse(i8);

#[derive(Default, Debug, Clone)]
pub struct Trigger;

#[derive(Default, Debug, Clone)]
pub(crate) struct PriceService {
    pub(crate) agent_handle: AgentHandle,
}

impl PriceService {
    pub(crate) async fn new(app: &mut AgentRuntime) -> Self {
        let config = AgentConfig::new(Ern::with_root("price_service").unwrap(), None, None).expect("Failed to create actor config");
        let mut price_service = app.create_actor_with_config::<PriceService>(config).await;
        price_service
            .act_on::<Pong>(|agent, context| {
                trace!("Received Pong");
                let envelope = context.reply_envelope();

                AgentReply::from_async(
                    async move {
                        trace!("Sending PriceResponse");
                        envelope.send(PongResponse(100)).await;
                    }
                )
            });

        let handle = price_service.start().await;
        PriceService {
            agent_handle: handle,
        }
    }
}

impl ShoppingCart {
    pub(crate) async fn new(price_service_handle: AgentHandle, app: &mut AgentRuntime) -> Self {
        // let mut shopping_cart = app.initialize::<ShoppingCart>().await;
        let config = AgentConfig::new(Ern::with_root("shopping_cart").unwrap(), None, None).expect("Failed to create actor config");
        let mut shopping_cart = app.create_actor_with_config::<ShoppingCart>(config).await;
        // Configure agent behavior
        shopping_cart
            .act_on::<Ping>(|agent, context| {
                let price_service = &agent.model.price_service_handle;
                let envelope = context.new_envelope(&price_service.reply_address());
                let name = price_service.name();

                AgentReply::from_async(async move {
                    trace!( "Sending to price_service id: {:?}", name);
                    envelope.send(Pong).await;
                })
            })
            .act_on::<PongResponse>(|agent, context| {
                let price = context.message().0;

                assert_eq!(price, 100);
                info!("fin. price: {}", price);

                AgentReply::immediate()
            });

        // set the model state
        shopping_cart.model.price_service_handle = price_service_handle.clone();

        let agent_handle = shopping_cart.start().await;

        ShoppingCart {
            agent_handle,
            price_service_handle,
        }
    }

    #[instrument(skip(self), level = "debug")]
    pub(crate) async fn trigger(&self) {
        //note: no need for &mut self because we only mutate internal state with message passing
        trace!("agent_handle id is {}", self.agent_handle.id().root.to_string());
        trace!("and pricing_service id is {}", self.price_service_handle.id().root.to_string());
        self.agent_handle.send(Ping).await;
    }
}
