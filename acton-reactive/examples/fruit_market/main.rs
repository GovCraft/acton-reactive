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

// Cash Register Example: A fun simulation of a grocery store checkout!
//
// This example shows how multiple agents can work together to create
// an interactive cash register system:
// - Scanning items and looking up their prices
// - Calculating totals and taxes
// - Displaying a receipt in real-time
// - Handling user input for a interactive experience

use std::io::{stdout, Write};
use std::sync::Once;

use anyhow::Result;
use crossterm::{
    cursor,
    event::{self, Event, KeyCode, KeyModifiers},
    execute, queue,
    terminal::{disable_raw_mode, enable_raw_mode, Clear, ClearType},
};
use futures::{SinkExt, StreamExt};
use tokio::sync::oneshot;
use tracing::Level;
use tracing::*;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter, FmtSubscriber};

use acton_reactive::prelude::*;
use cart_item::CartItem;
use register::Register;

// Import our other modules that make up the system
mod cart_item; // Handles shopping cart items
mod price_service; // Looks up prices for items
mod printer; // Displays the receipt
mod register; // Manages the whole transaction

use crate::cart_item::Price;

// Constants to make our code more readable
const FAILED_TO_ENABLE_RAW_MODE: &str = "Failed to enable raw mode";
const FAILED_TO_DISABLE_RAW_MODE: &str = "Failed to disable raw mode";
const SHUTDOWN_MESSAGE: &str = "Shutting down...\n";
const ERROR_READING_EVENT: &str = "Error reading event: {:?}";
const LOG_DIRECTORY: &str = "logs";
const LOG_FILENAME: &str = "tracing.log";

// Messages that our agents can send to each other
#[derive(Clone, Debug)]
struct ItemScanned(CartItem); // When we scan a new item

#[derive(Clone, Debug)]
struct FinalizeSale(Price); // When we're done and ready to total up

#[derive(Clone, Debug)]
struct GetItems; // Request to see current items

#[derive(Clone, Debug)]
struct PriceRequest(CartItem); // Ask for an item's price

#[derive(Clone, Debug)]
struct PriceResponse {
    // Response with the price
    item: CartItem,
}

#[derive(Clone, Debug)]
enum PrinterMessage {
    // Tell the printer to update display
    Repaint,
}

// Helper to make sure we clean up terminal settings
struct RawModeGuard;

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        disable_raw_mode().expect(FAILED_TO_DISABLE_RAW_MODE);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set up our logging system
    initialize_tracing();
    info!("** App startup **");

    // Set up the terminal for our interactive display
    enable_raw_mode().expect(FAILED_TO_ENABLE_RAW_MODE);
    let _raw_mode_guard = RawModeGuard;
    let mut stdout = stdout();
    execute!(stdout, Clear(ClearType::All), cursor::MoveTo(0, 0))?;

    // Start our cash register system
    let mut app = ActonApp::launch();
    let register = Register::new_transaction(&mut app).await?.clone();

    // Set up a way to gracefully shut down
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Handle keyboard input in a separate task
    tokio::spawn(async move {
        let mut reader = event::EventStream::new();
        while let Some(event_result) = reader.next().await {
            match event_result {
                Ok(Event::Key(key_event)) => match key_event.code {
                    // Ctrl+C or 'q' to quit
                    KeyCode::Char('c') if key_event.modifiers.contains(KeyModifiers::CONTROL) => {
                        let _ = shutdown_tx.send(());
                        break;
                    }
                    KeyCode::Char('q') => {
                        let _ = shutdown_tx.send(());
                        break;
                    }
                    // 's' to scan an item
                    KeyCode::Char('s') => {
                        register.scan().await.expect("Scan item failed");
                    }
                    // '?' to toggle help display
                    KeyCode::Char('?') => {
                        register.toggle_help().await.expect("Toggle help failed");
                    }
                    _ => {}
                },
                Err(e) => {
                    eprintln!("{} {}", ERROR_READING_EVENT, e);
                    break;
                }
                _ => {}
            }
        }
    });

    // Wait for shutdown signal
    shutdown_rx.await?;

    // Clean up the display
    queue!(stdout, cursor::MoveTo(0, 0))?;
    queue!(stdout, Clear(ClearType::FromCursorDown))?;
    stdout.write_all(SHUTDOWN_MESSAGE.as_bytes())?;
    queue!(stdout, cursor::Show)?;
    stdout.flush()?;
    queue!(stdout, cursor::MoveTo(0, 1))?;

    // Shut down all our agents
    app.shutdown_all().await?;
    info!("Shutdown complete.");

    Ok(())
}

// Set up our logging system (only needs to happen once)
static INIT: Once = Once::new();

pub fn initialize_tracing() {
    INIT.call_once(|| {
        // Configure what we want to log
        let filter = EnvFilter::new("")
            .add_directive("replies=debug".parse().unwrap())
            .add_directive("replies::printer=debug".parse().unwrap())
            .add_directive("replies::shopping_cart=off".parse().unwrap())
            .add_directive("replies::price_service=off".parse().unwrap())
            .add_directive("acton_core::common::agent_handle=off".parse().unwrap())
            .add_directive("acton_core::common::agent_broker=off".parse().unwrap())
            .add_directive(
                "acton_core::actor::managed_agent::idle[start]=off"
                    .parse()
                    .unwrap(),
            )
            .add_directive(
                "acton_core::actor::managed_agent::started[wake]=off"
                    .parse()
                    .unwrap(),
            )
            .add_directive(
                "acton_core::traits::actor[send_message]=off"
                    .parse()
                    .unwrap(),
            )
            .add_directive("supervisor_tests=off".parse().unwrap())
            .add_directive("broker_tests=off".parse().unwrap())
            .add_directive("launchpad_tests=off".parse().unwrap())
            .add_directive("lifecycle_tests=off".parse().unwrap())
            .add_directive("actor_tests=off".parse().unwrap())
            .add_directive("load_balancer_tests=off".parse().unwrap())
            .add_directive("acton::tests::setup::actor::pool_item=off".parse().unwrap());

        // Set up log file
        let file_appender = RollingFileAppender::new(Rotation::DAILY, LOG_DIRECTORY, LOG_FILENAME);

        // Configure how we want to format our logs
        let subscriber = FmtSubscriber::builder()
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
            .expect("Setting default subscriber failed");
    });
}
