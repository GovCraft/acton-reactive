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

use futures::SinkExt;
use tracing::*;
use tracing::Level;
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use tracing_subscriber::fmt::format::FmtSpan;

use acton::prelude::*;
use cart_item::CartItem;
use register::Register;
use shopping_cart::ShoppingCart;

use crate::cart_item::Price;
use crate::printer::Printer;

mod shopping_cart;
mod price_service;
mod cart_item;
mod register;
mod printer;

// Define messages to interact with the agent.
#[derive(Clone, Debug)]
struct ItemScanned(CartItem);

#[derive(Clone, Debug)]
struct FinalizeSale(pub(crate) Price);


#[derive(Clone, Debug)]
struct GetItems;

#[derive(Clone, Debug)]
struct GetPriceRequest(CartItem);

#[acton_message]
struct PriceResponse {
    price: i32,
    item: CartItem,
}

#[acton_message]
struct Status<'a> {
    message: &'a str,
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    initialize_tracing();
    // Set up the system and create agents
    let mut app = ActonApp::launch();
    let cashier_register = Register::new_transaction(ShoppingCart::new(&mut app).await?);
    let _printer = Printer::power_on(&mut app).await;

    cashier_register.scan("Banana", 3).await;
    cashier_register.scan("Apple", 1).await;
    cashier_register.scan("Cantaloupe", 2).await;

    // tokio::time::sleep( std::time::Duration::from_secs(1)).await;
    // Shut down the system and all agents
    app.shutdown_all().await.expect("Failed to shut down system");
    Ok(())
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
            .add_directive("replies=error".parse().unwrap())
            .add_directive("replies::printer=error".parse().unwrap())
            .add_directive("replies::shopping_cart=debug".parse().unwrap())
            .add_directive("replies::price_service=error".parse().unwrap())
            //acton core
            .add_directive("acton_core::common::agent_handle=error".parse().unwrap())
            .add_directive("acton_core::common::agent_broker=error".parse().unwrap())
            .add_directive("acton_core::actor::managed_agent::idle[start]=off".parse().unwrap())
            .add_directive("acton_core::actor::managed_agent::started[wake]=error".parse().unwrap())
            .add_directive("acton_core::traits::actor[send_message]=error".parse().unwrap())
            //tests
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
            // .add_directive("replies=info".parse().unwrap())
            .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into()); // Set global log level to TRACE

        let subscriber = FmtSubscriber::builder()
            // .with_span_events(FmtSpan::ENTER | FmtSpan::EXIT)
            .with_span_events(FmtSpan::NONE)
            .with_max_level(Level::TRACE)
            .compact()
            .with_line_number(true)
            .with_target(true)
            .without_time()
            .with_env_filter(filter)
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    });
}
