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

// Cart Item Module: Represents items in our shopping cart
//
// This module defines how we store and handle items in our cart:
// - What information we track for each item
// - How to display items nicely
// - How to handle money calculations
// - How to create unique IDs for items

use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::ops::{AddAssign, Div, Mul};

use mti::prelude::*;

// CartItem: Everything we need to know about an item in our cart
#[derive(Default, Debug, Clone)]
pub(crate) struct CartItem {
    name: String,     // What the item is called
    quantity: i32,    // How many of this item
    cost: Cost,       // Cost per item
    upc: MagicTypeId, // Unique ID for this type of item
}

impl CartItem {
    // Create a new cart item with a name and quantity
    pub(crate) fn new(name: impl Into<String>, quantity: i32) -> Self {
        let name = name.into();
        // Create a unique ID based on the item name
        let mut upc = "upc_".to_string();
        upc.push_str(&*name.clone());

        CartItem {
            name: name.into(),
            quantity,
            upc: upc.create_type_id::<V7>(),
            ..Default::default()
        }
    }

    // Getter methods to safely access item properties
    pub(crate) fn id(&self) -> &MagicTypeId {
        &self.upc
    }

    pub(crate) fn name(&self) -> &String {
        &self.name
    }

    pub(crate) fn quantity(&self) -> i32 {
        self.quantity
    }

    // Calculate the total price for this item (cost × quantity)
    pub(crate) fn price(&self) -> Price {
        let price = &self.cost * self.quantity;
        Price(price)
    }

    pub(crate) fn cost(&self) -> &Cost {
        &self.cost
    }

    // Setter methods to safely modify item properties
    pub(crate) fn set_quantity(&mut self, quantity: i32) {
        self.quantity = quantity;
    }

    pub(crate) fn set_cost(&mut self, cost: i32) {
        self.cost = Cost(cost);
    }
}

// How to display a cart item as text
impl Display for CartItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} x {} @ {}", self.quantity, self.name, self.cost)
    }
}

// Cost: Handles money amounts for individual items
#[derive(Clone, Debug, Default)]
pub(crate) struct Cost(i32); // Stored in cents

impl Deref for Cost {
    type Target = i32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// How to display costs as money
impl Display for Cost {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        format_money(self.0, f)
    }
}

// Math operations for Cost

// Multiply cost by a quantity
impl Mul<i32> for Cost {
    type Output = ();

    fn mul(self, rhs: i32) -> Self::Output {
        self.0 * rhs;
    }
}

// Get the raw cents value
impl AsRef<i32> for Cost {
    fn as_ref(&self) -> &i32 {
        &self.0
    }
}

// Multiply a reference to cost by a quantity
impl Mul<i32> for &Cost {
    type Output = i32;

    fn mul(self, rhs: i32) -> Self::Output {
        self.0 * rhs
    }
}

// Divide a reference to cost by a number
impl Div<i32> for &Cost {
    type Output = Cost;

    fn div(self, rhs: i32) -> Self::Output {
        Cost(self.0 / rhs)
    }
}

// Price: The total cost for an item or group of items
#[derive(Default, Debug, Clone)]
pub(crate) struct Price(pub(crate) i32); // Stored in cents

// How to display a price as money
impl Display for crate::cart_item::Price {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_money(self.0, f)
    }
}

// Allow adding a Price to an i32 (for totaling)
impl AddAssign<Price> for i32 {
    fn add_assign(&mut self, rhs: Price) {
        *self += rhs.0;
    }
}

// Helper function to format cents as dollars and cents
fn format_money(cents: i32, f: &mut Formatter) -> Result<(), std::fmt::Error> {
    write!(f, "${}.{:02}", cents / 100, cents % 100)
}
