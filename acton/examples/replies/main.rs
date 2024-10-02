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

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use futures::SinkExt;
use futures::StreamExt;
use mti::prelude::MagicTypeId;
use tokio::sync::oneshot;
use tracing::*;
use tracing::Level;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use tracing_subscriber::fmt::format::FmtSpan;

use acton::prelude::*;
use cart_item::CartItem;
use register::Register;
use shopping_cart::ShoppingCart;

use crate::cart_item::Price;
use crate::frame_runner::FrameRunner;
use crate::printer::Printer;

mod shopping_cart;
mod price_service;
mod cart_item;
mod register;
mod printer;
mod frame_runner;

// Define messages to interact with the agent.
#[derive(Clone, Debug)]
struct ItemScanned(CartItem);

#[derive(Clone, Debug)]
struct FinalizeSale(pub(crate) Price);


#[derive(Clone, Debug)]
struct GetItems;

#[derive(Clone, Debug)]
struct GetPriceRequest(CartItem);

#[derive(Clone, Debug)]
struct PriceResponse {
    item: CartItem,
}

#[derive(Clone, Debug)]
enum PrinterMessage {
    Help(&'static str),
    Status(&'static str),
    PrintLine(&'static str),
    Loading(MagicTypeId),
}


struct RawModeGuard;

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        disable_raw_mode().expect("Failed to disable raw mode");
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    initialize_tracing();
    info!("\n** App startup **");
    // Enable raw mode to prevent "^C" from being printed when Control-C is pressed
    enable_raw_mode().expect("Failed to enable raw mode");

    // Create a guard to ensure raw mode is disabled when the program exits
    let _raw_mode_guard = RawModeGuard;


    // Set up the system and create agents
    let mut app = ActonApp::launch();
    let cashier_register = Register::new_transaction(ShoppingCart::new(&mut app).await?);
    let mut printer = Printer::power_on(&mut app).await?;



    // Perform scanning operations
    cashier_register.scan("Banana", 3).await;
    cashier_register.scan("Apple", 1).await;
    cashier_register.scan("Cantaloupe", 2).await;
    // cashier_register.scan("Orange", 4).await;
    // cashier_register.scan("Grapes", 2).await;
    // cashier_register.scan("Mango", 5).await;
    // cashier_register.scan("Pineapple", 1).await;
    // cashier_register.scan("Strawberry", 6).await;


    // Create a channel to signal shutdown
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Spawn a task to listen for Ctrl-C key event
    tokio::spawn(async move {
        let mut reader = event::EventStream::new();
        while let Some(event_result) = reader.next().await {
            match event_result {
                Ok(Event::Key(key_event)) => {
                    if key_event.code == KeyCode::Char('c') && key_event.modifiers.contains(event::KeyModifiers::CONTROL) {
                        // Send shutdown signal
                        let _ = shutdown_tx.send(());
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Error reading event: {:?}", e);
                    break;
                }
                _ => {}
            }
        }
    });

    // Wait for the shutdown signal
    shutdown_rx.await?;
    printer.send(PrinterMessage::Status("Control-C received. Shutting down...")).await;


    // Shut down the system and all agents
    app.shutdown_all().await?;

    info!("Shutdown complete.");

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
            .add_directive("replies=debug".parse().unwrap())
            .add_directive("replies::printer=trace".parse().unwrap())
            .add_directive("replies::frame_runner=trace".parse().unwrap())
            .add_directive("replies::shopping_cart=off".parse().unwrap())
            .add_directive("replies::price_service=off".parse().unwrap())
            //acton core
            .add_directive("acton_core::common::agent_handle=off".parse().unwrap())
            .add_directive("acton_core::common::agent_broker=off".parse().unwrap())
            .add_directive("acton_core::actor::managed_agent::idle[start]=off".parse().unwrap())
            .add_directive("acton_core::actor::managed_agent::started[wake]=off".parse().unwrap())
            .add_directive("acton_core::traits::actor[send_message]=off".parse().unwrap())
            //tests
            .add_directive("supervisor_tests=off".parse().unwrap())
            .add_directive("broker_tests=off".parse().unwrap())
            .add_directive("launchpad_tests=off".parse().unwrap())
            .add_directive("lifecycle_tests=off".parse().unwrap())
            .add_directive("actor_tests=off".parse().unwrap())
            .add_directive("load_balancer_tests=off".parse().unwrap())
            .add_directive(
                "acton::tests::setup::actor::pool_item=off"
                    .parse()
                    .unwrap(),
            );
            // .add_directive("replies=trace".parse().unwrap())
            // .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into()); // Set global log level to TRACE
        // Set up the file appender
        let file_appender = RollingFileAppender::new(Rotation::DAILY, "logs", "tracing.log");

        let subscriber = FmtSubscriber::builder()
            // .with_span_events(FmtSpan::ENTER | FmtSpan::EXIT)
            .with_span_events(FmtSpan::NONE)
            .with_max_level(Level::TRACE)
            .compact()
            .with_line_number(false)
            .with_target(false)
            .without_time()
            .with_env_filter(filter)
            .with_writer(file_appender)
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    });
}
