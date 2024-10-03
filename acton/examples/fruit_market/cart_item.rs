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
use std::fmt::{Display, Formatter};
use std::ops::{AddAssign, Div, Mul};
use std::ops::Deref;

use mti::prelude::*;

#[derive(Default, Debug, Clone)]
pub(crate) struct CartItem {
    name: String,
    quantity: i32,
    cost: Cost,
    upc: MagicTypeId,
}


impl CartItem {
    pub(crate) fn new(name: impl Into<String>, quantity: i32) -> Self {
        let name = name.into();
        let mut upc = "upc_".to_string();
        upc.push_str(&*name.clone());
        CartItem {
            name: name.into(),
            quantity,
            upc: upc.create_type_id::<V7>(),
            ..Default::default()
        }
    }

    pub(crate) fn id(&self) -> &MagicTypeId {
        &self.upc
    }

    pub(crate) fn name(&self) -> &String {
        &self.name
    }

    pub(crate) fn quantity(&self) -> i32 {
        self.quantity
    }

    pub(crate) fn price(&self) -> Price {
        let price = &self.cost * self.quantity;
        Price(price)
    }
    pub(crate) fn cost(&self) -> &Cost {
        &self.cost
    }

    pub(crate) fn set_quantity(&mut self, quantity: i32) {
        self.quantity = quantity;
    }

    pub(crate) fn set_cost(&mut self, cost: i32) {
        self.cost = Cost(cost);
    }
}

impl Display for CartItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Assuming `format_money` is a function that takes an `i32` and `Formatter`
        write!(f, "{} x {} @ {}", self.quantity, self.name, self.cost)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Cost(i32);

impl Deref for Cost {
    type Target = i32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Implement Display for Cost to format it properly
impl Display for Cost {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Assuming `format_money` is a function that takes an `i32` and `Formatter`
        format_money(self.0, f)
    }
}

impl Mul<i32> for Cost {
    type Output = ();

    fn mul(self, rhs: i32) -> Self::Output {
        self.0 * rhs;
    }
}


impl AsRef<i32> for Cost {
    fn as_ref(&self) -> &i32 {
        &self.0
    }
}

impl Mul<i32> for &Cost {
    type Output = i32;

    fn mul(self, rhs: i32) -> Self::Output {
        self.0 * rhs
    }
}


// Implement Div to return a new Cost instance
impl Div<i32> for &Cost {
    type Output = Cost;

    fn div(self, rhs: i32) -> Self::Output {
        Cost(self.0 / rhs)
    }
}

#[derive(Default, Debug, Clone)]
pub(crate) struct Price(pub(crate) i32);

impl Display for crate::cart_item::Price {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_money(self.0, f)
    }
}

impl AddAssign<Price> for i32 {
    fn add_assign(&mut self, rhs: Price) {
        *self += rhs.0;
    }
}


fn format_money(cents: i32, f: &mut Formatter) -> Result<(), std::fmt::Error> {
    write!(f, "${}.{}", cents / 100, cents % 100)
}
