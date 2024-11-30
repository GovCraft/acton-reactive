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

// Register Module: The main control center for our cash register
//
// This module ties everything together:
// - Creates and manages our price service and printer
// - Handles scanning items
// - Simulates a real grocery store checkout
// - Provides a simple interface for the main program
//
// Think of it like the main control panel of a cash register!

use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use tracing::{debug, trace};

use acton_core::prelude::*;

use crate::printer::{Printer, ToggleHelp};
use crate::{price_service::PriceService, CartItem, ItemScanned};

// List of items that can be "scanned"
// In a real system, this would come from a product database
const GROCERY_ITEMS: &[&str] = &[
    "Apple",
    "Banana",
    "Cantaloupe",
    "Orange",
    "Grapes",
    "Mango",
    "Pineapple",
    "Strawberry",
    "Milk",
    "Bread",
];

// Simulation constants
const QUANTITY_MIN: i32 = 1; // Minimum items to add
const QUANTITY_MAX: i32 = 6; // Maximum items to add
const ITEM_SELECTION_ERROR: &str = "Failed to select an item";

// The Register struct manages our cash register system
#[derive(Clone)]
pub struct Register {
    pub(crate) price_service: AgentHandle, // Handles price lookups
    pub(crate) printer: AgentHandle,       // Handles display
}

impl Register {
    // Create a new cash register transaction
    pub async fn new_transaction(app: &mut AgentRuntime) -> anyhow::Result<Self> {
        Ok(Register {
            // Start up our printer first (it displays everything)
            printer: Printer::power_on(app).await?,
            // Then start our price service (it looks up prices)
            price_service: PriceService::new(app).await?,
        })
    }

    // Toggle the help display
    pub async fn toggle_help(&self) -> anyhow::Result<()> {
        trace!("Register::toggle_help");
        self.printer.send(ToggleHelp).await;
        Ok(())
    }

    // Simulate scanning an item
    // In a real system, this would read from a barcode scanner
    pub async fn scan(&self) -> anyhow::Result<()> {
        // Create a random number generator
        let mut rng = StdRng::from_entropy();

        // Pick a random item from our list
        let item_name = GROCERY_ITEMS
            .choose(&mut rng)
            .expect(ITEM_SELECTION_ERROR)
            .to_string();

        // Pick a random quantity between MIN and MAX
        let quantity = rng.gen_range(QUANTITY_MIN..=QUANTITY_MAX);

        // Create a cart item and send it to the price service
        self.price_service
            .send(ItemScanned(CartItem::new(item_name, quantity)))
            .await;
        Ok(())
    }
}
