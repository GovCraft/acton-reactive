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

use tracing::{debug, error};

use acton_core::prelude::{AgentHandle, AgentReply, AgentRuntime, Subscribable};

use crate::{PriceResponse, Status};

#[derive(Default, Debug, Clone)]
pub struct Printer;

impl Printer {
    pub async fn power_on(app: &mut AgentRuntime) -> AgentHandle {
        let mut agent = app.new_agent::<Printer>().await;
        agent.act_on::<PriceResponse>(|agent, context| {
            debug!("Received a message: {:?}", context.message());
            let item = &context.message().item;
            eprintln!("{item}");
            AgentReply::immediate()
        })
            .act_on::<Status>(|agent, context| {
                let status = &context.message().message;
                eprintln!("Status: {status}");
                AgentReply::immediate()
            });

        debug!("Subscribing to GetPriceResponse");
        agent.handle().subscribe::<PriceResponse>().await;
        agent.handle().subscribe::<Status>().await;
        agent.start().await
    }
}