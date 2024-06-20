/*
 *
 *  *
 *  * Copyright (c) 2024 Govcraft.
 *  *
 *  *  Licensed under the Business Source License, Version 1.1 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *      https://github.com/GovCraft/akton-framework/tree/main/LICENSES
 *  *
 *  *  Change Date: Three years from the release date of this version of the Licensed Work.
 *  *  Change License: Apache License, Version 2.0
 *  *
 *  *  Usage Limitations:
 *  *    - You may use the Licensed Work for non-production purposes only, such as internal testing, development, and experimentation.
 *  *    - You may not use the Licensed Work for any production or commercial purpose, including, but not limited to, the provision of any service to third parties, without a commercial use license from the Licensor, except as stated in the Exemptions section of the License.
 *  *
 *  *  Exemptions:
 *  *    - Open Source Projects licensed under an OSI-approved open source license.
 *  *    - Non-Profit Organizations using the Licensed Work for non-commercial purposes.
 *  *    - Small For-Profit Companies with annual gross revenues not exceeding $2,000,000 USD.
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
use std::sync::Once;

use tracing::Level;
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use tracing_subscriber::fmt::format::FmtSpan;

pub use actors::*;
pub use messages::*;

mod actors;
mod messages;

static INIT: Once = Once::new();

pub fn init_tracing() {
    INIT.call_once(|| {
        // Define an environment filter to suppress logs from the specific function

        // let filter = EnvFilter::new("")
        //     // .add_directive("akton_core::common::context::emit_pool=trace".parse().unwrap())
        //     // .add_directive("akton_core::common::context::my_func=trace".parse().unwrap())
        //     .add_directive("akton_core::common::context[my_func]=trace".parse().unwrap())
        //     .add_directive(Level::INFO.into()); // Set global log level to INFO

        let filter = EnvFilter::new("")
            .add_directive("akton_core::common::context=error".parse().unwrap())
            .add_directive("akton_core::common::akton=error".parse().unwrap())
            .add_directive("akton_core::pool=error".parse().unwrap())
            .add_directive("akton_core::pool::builder=error".parse().unwrap())
            .add_directive("akton_core::common::system=error".parse().unwrap())
            .add_directive("akton_core::common::supervisor=error".parse().unwrap())
            .add_directive("akton_core::actors::actor=trace".parse().unwrap())
            .add_directive("akton_core::common::broker=trace".parse().unwrap())
            .add_directive("akton_core::actors::actor=off".parse().unwrap())
            .add_directive("akton_core::actors::actor[wake]=off".parse().unwrap())
            .add_directive("akton_core::actors::actor[handle_message]=trace".parse().unwrap())
            .add_directive("akton_core::actors::actor[suspend_self]=off".parse().unwrap())
            .add_directive("akton_core::actors::actor[new]=error".parse().unwrap())
            .add_directive("akton_core::actors::actor[init]=error".parse().unwrap())
            .add_directive("akton_core::actors::actor[activate]=error".parse().unwrap())
            .add_directive("akton_core::actors::idle=error".parse().unwrap())
            .add_directive("akton_core::actors::idle[act_on_async]=trace".parse().unwrap())
            .add_directive("akton_core::actors::idle[act_on]=debug".parse().unwrap())
            .add_directive("akton_core::message::outbound_envelope=off".parse().unwrap())
            .add_directive("akton_core::message::broadcast_envelope=debug".parse().unwrap())
            .add_directive("akton_core::traits::actor_context=error".parse().unwrap())
            .add_directive("akton_core::traits::subscribable=trace".parse().unwrap())
            .add_directive("supervisor_tests=info".parse().unwrap())
            .add_directive("broker_tests=trace".parse().unwrap())
            .add_directive("lifecycle_tests=info".parse().unwrap())
            .add_directive("actor_tests=info".parse().unwrap())
            .add_directive("load_balancer_tests=info".parse().unwrap())
            .add_directive("akton::tests::setup::actors::pool_item=info".parse().unwrap())
            .add_directive("messaging_tests=info".parse().unwrap())
            ;
        // .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into()); // Set global log level to TRACE

        let subscriber = FmtSubscriber::builder()
            // .with_span_events(FmtSpan::ENTER | FmtSpan::EXIT)
            .with_span_events(FmtSpan::NONE)
            .with_max_level(Level::TRACE)
            .compact()
            .with_line_number(true)
            .without_time()
            .with_env_filter(filter)
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    });
}
