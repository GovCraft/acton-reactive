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

use std::sync::Once;

use tracing::*;
use tracing::Level;
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use tracing_subscriber::fmt::format::FmtSpan;

use acton::prelude::*;
use cart_item::CartItem;
use price_service::PriceService;
use shopping_cart::ShoppingCart;

mod shopping_cart;
mod price_service;
mod cart_item;

// Define messages to interact with the agent.
#[derive(Clone, Debug)]
struct AddItem(CartItem);

#[derive(Clone, Debug)]
struct GetItems;

#[derive(Clone, Debug)]
struct GetPriceRequest(CartItem);

#[derive(Clone, Debug)]
struct GetPriceResponse {
    price: usize,
    item: CartItem,
}

#[tokio::main]
async fn main() {
    initialize_tracing();
    // Set up the system and create agents
    let mut app = ActonApp::launch();
    let price_service = PriceService::new(&mut app).await;
    let price_service_handle = price_service.handle.clone();
    let shopping_cart = ShoppingCart::new(price_service_handle, &mut app).await;
debug!("shopping_cart has pricing_service with id: {}", shopping_cart.price_service_handle.id());
    shopping_cart.add_item("Apple", 1).await;
    shopping_cart.add_item("Banana", 2).await;
    shopping_cart.add_item("Cantaloupe", 3).await;


    // Shut down the system and all agents
    app.shutdown_all().await.expect("Failed to shut down system");
}

static INIT: Once = Once::new();

pub fn initialize_tracing() {
    INIT.call_once(|| {
        // Define an environment filter to suppress logs from the specific function

        // let filter = EnvFilter::new("")
        //     // .add_directive("acton_core::common::context::emit_pool=trace".parse().unwrap())
        //     // .add_directive("acton_core::common::context::my_func=trace".parse().unwrap())
        //     .add_directive("acton_core::common::context[my_func]=trace".parse().unwrap())
        //     .add_directive(Level::INFO.into()); // Set global log level to INFO

        let filter = EnvFilter::new("")
            .add_directive("replies=off".parse().unwrap())
            .add_directive("replies::shopping_cart=off".parse().unwrap())
            .add_directive("replies::price_service=off".parse().unwrap())
            .add_directive("acton_core::common::agent_handle[return_address]=off".parse().unwrap())
            .add_directive("acton_core::actor::managed_agent::idle[start]=debug".parse().unwrap())
            .add_directive("acton_core::actor::managed_agent::started[wake]=off".parse().unwrap())
            .add_directive("acton_core::traits::actor[send_message]=debug".parse().unwrap())
            .add_directive(
                "acton_core::message::outbound_envelope[reply_message_async]=debug"
                    .parse()
                    .unwrap(),
            )
            .add_directive("acton_core::common::actor_ref=off".parse().unwrap())
            .add_directive("acton_core::common::acton=off".parse().unwrap())
            .add_directive("acton_core::pool=off".parse().unwrap())
            .add_directive("acton_core::pool::builder=off".parse().unwrap())
            .add_directive("acton_core::common::system=off".parse().unwrap())
            .add_directive("acton_core::common::supervisor=off".parse().unwrap())
            .add_directive("acton_core::common::broker=off".parse().unwrap())
            .add_directive(
                "acton_core::common::broker[broadcast]=off"
                    .parse()
                    .unwrap(),
            )
            .add_directive("acton_core::actor::managed_actor=off".parse().unwrap())
            .add_directive("acton_core::actor::actor[wake]=off".parse().unwrap())
            .add_directive(
                "acton_core::actor::actor[terminate_actor]=off"
                    .parse()
                    .unwrap(),
            )
            .add_directive(
                "acton_core::actor::actor[handle_message]=off"
                    .parse()
                    .unwrap(),
            )
            .add_directive(
                "acton_core::actor::actor[suspend_self]=off"
                    .parse()
                    .unwrap(),
            )
            .add_directive("acton_core::actor::actor[new]=off".parse().unwrap())
            .add_directive("acton_core::actor::actor[init]=off".parse().unwrap())
            .add_directive("acton_core::actor::actor[activate]=off".parse().unwrap())
            .add_directive("acton_core::actor::idle=off".parse().unwrap())
            .add_directive("acton_core::actor::idle[act_on_async]=off".parse().unwrap())
            .add_directive("acton_core::actor::idle[act_on]=off".parse().unwrap())
            .add_directive(
                "acton_core::message::broadcast_envelope=off"
                    .parse()
                    .unwrap(),
            )
            .add_directive("acton_core::traits::broker_context=off".parse().unwrap())
            .add_directive("acton_core::traits::subscribable=off".parse().unwrap())
            .add_directive("supervisor_tests=off".parse().unwrap())
            .add_directive("broker_tests=trace".parse().unwrap())
            .add_directive("launchpad_tests=off".parse().unwrap())
            .add_directive("lifecycle_tests=off".parse().unwrap())
            .add_directive("actor_tests=off".parse().unwrap())
            .add_directive("load_balancer_tests=off".parse().unwrap())
            .add_directive(
                "acton::tests::setup::actor::pool_item=off"
                    .parse()
                    .unwrap(),
            )
            .add_directive("messaging_tests=off".parse().unwrap());
        // .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into()); // Set global log level to TRACE

        let subscriber = FmtSubscriber::builder()
            // .with_span_events(FmtSpan::ENTER | FmtSpan::EXIT)
            .with_span_events(FmtSpan::NONE)
            .with_max_level(Level::TRACE)
            .compact()
            .with_line_number(true)
            .with_target(false)
            .without_time()
            .with_env_filter(filter)
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    });
}
