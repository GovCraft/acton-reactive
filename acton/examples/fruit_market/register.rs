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


use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use tracing::debug;

use acton_core::prelude::*;

use crate::{price_service::PriceService, CartItem, ItemScanned};
use crate::printer::{Printer, ToggleHelp};

const GROCERY_ITEMS: &[&str] = &[
    "Apple", "Banana", "Cantaloupe", "Orange", "Grapes", "Mango",
    "Pineapple", "Strawberry", "Milk", "Bread",
];
const QUANTITY_MIN: i32 = 1;
const QUANTITY_MAX: i32 = 6;
const ITEM_SELECTION_ERROR: &str = "Failed to select an item";
#[derive(Clone)]
pub struct Register {
    pub(crate) price_service: AgentHandle,
    pub(crate) printer: AgentHandle,
}

impl Register {
    pub async fn new_transaction(app: &mut AgentRuntime) -> anyhow::Result<Self> {
        Ok(Register {
            printer: Printer::power_on(app).await?,
            price_service: PriceService::new(app).await?,
        })
    }

    pub async fn toggle_help(&self) -> anyhow::Result<()> {
        debug!("Register::toggle_help");
        self.printer.send(ToggleHelp).await;
        Ok(())
    }

    pub async fn scan(&self) -> anyhow::Result<()> {
        let mut rng = StdRng::from_entropy();

        let item_name = GROCERY_ITEMS
            .choose(&mut rng)
            .expect(ITEM_SELECTION_ERROR)
            .to_string();

        let quantity = rng.gen_range(QUANTITY_MIN..=QUANTITY_MAX);

        self.price_service
            .send(ItemScanned(CartItem::new(item_name, quantity)))
            .await;
        Ok(())
    }
}
