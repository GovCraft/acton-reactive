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

use acton_ern::prelude::*;
use derive_new::new;

use crate::common::Outbox;

/// Message address with a sender id
#[derive(new, Clone, Debug)]
pub struct MessageAddress {
    pub(crate) address: Outbox,
    pub(crate) sender: Ern,
}

impl MessageAddress {
    /// get address owner
    pub fn name(&self) -> &str {
        self.sender.root.as_str()
    }
}

impl Default for MessageAddress {
    fn default() -> Self {
        let (outbox, _) = tokio::sync::mpsc::channel(1);
        Self::new(outbox, Ern::default())
    }
}

// write unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_return_address() {
        let return_address = MessageAddress::default();
        assert!(return_address.address.is_closed());
    }
}