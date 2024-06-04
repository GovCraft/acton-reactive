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
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::FmtSubscriber;

pub use messages::*;

mod messages;
mod actors;
pub use actors::*;
static INIT: Once = Once::new();

pub fn init_tracing() {
    INIT.call_once(|| {
        // Define an environment filter to suppress logs from the specific function
        let filter = tracing_subscriber::EnvFilter::new("")
            .add_directive("akton_core::common::context::peek_state_span=off".parse().unwrap())
            .add_directive("akton_core::common::context=trace".parse().unwrap())
            .add_directive("tests=off".parse().unwrap())
            .add_directive("actor_tests=trace".parse().unwrap())
            .add_directive("akton_core::traits=trace".parse().unwrap())
            .add_directive("akton_core::common::awake=off".parse().unwrap())
            .add_directive("akton_core::common::akton=off".parse().unwrap())
            .add_directive("akton_core::common::pool_builder=off".parse().unwrap())
            .add_directive("akton_core::common::system=off".parse().unwrap())
            .add_directive("akton_core::common::supervisor=off".parse().unwrap())
            .add_directive("akton_core::common::actor=trace".parse().unwrap())
            .add_directive("akton_core::common::idle=off".parse().unwrap())
            .add_directive("akton_core::common::outbound_envelope=trace".parse().unwrap())
            .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into()); // Set global log level to TRACE

        let subscriber = FmtSubscriber::builder()
            // .with_span_events(FmtSpan::ENTER | FmtSpan::EXIT)
            // .with_span_events(FmtSpan::EXIT)
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
