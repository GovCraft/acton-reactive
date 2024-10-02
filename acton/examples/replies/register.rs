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


use acton_core::prelude::{Actor, AgentHandle, Broker};

use crate::{ItemScanned};
use crate::cart_item::CartItem;

pub struct Register(pub AgentHandle);

impl Register {
    pub fn new_transaction(shopping_cart: AgentHandle) -> Self {
        Register(shopping_cart)
    }
    pub async fn scan(&self, name: impl Into<String>, quantity: i32) {
        self.0.send(ItemScanned(CartItem::new(name, quantity))).await;
    }

}