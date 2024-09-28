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
use std::sync::Once;

use tracing::Level;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

pub use actors::*;
pub use messages::*;

mod actors;
mod messages;

static INIT: Once = Once::new();

pub fn initialize_tracing() {
    INIT.call_once(|| {
        // Define an environment filter to suppress logs from the specific function

        // let filter = EnvFilter::new("")
        //     // .add_directive("acton_core::common::context::emit_pool=trace".parse().unwrap())
        //     // .add_directive("acton_core::common::context::my_func=trace".parse().unwrap())
        //     .add_directive("acton_core::common::context[my_func]=trace".parse().unwrap())
        //     .add_directive(Level::INFO.into()); // Set global log level to INFO

        let filter = EnvFilter::new("")
            .add_directive("acton_core::common::actor_ref=debug".parse().unwrap())
            .add_directive("acton_core::common::acton=error".parse().unwrap())
            .add_directive("acton_core::pool=error".parse().unwrap())
            .add_directive("acton_core::pool::builder=error".parse().unwrap())
            .add_directive("acton_core::common::system=error".parse().unwrap())
            .add_directive("acton_core::common::supervisor=error".parse().unwrap())
            .add_directive("acton_core::actor::actor=error".parse().unwrap())
            .add_directive("acton_core::common::broker=error".parse().unwrap())
            .add_directive(
                "acton_core::common::broker[broadcast]=error"
                    .parse()
                    .unwrap(),
            )
            .add_directive("acton_core::actor::managed_actor=info".parse().unwrap())
            .add_directive("acton_core::actor::actor[wake]=error".parse().unwrap())
            .add_directive(
                "acton_core::actor::actor[terminate_actor]=off"
                    .parse()
                    .unwrap(),
            )
            .add_directive(
                "acton_core::actor::actor[handle_message]=trace"
                    .parse()
                    .unwrap(),
            )
            .add_directive(
                "acton_core::actor::actor[suspend_self]=off"
                    .parse()
                    .unwrap(),
            )
            .add_directive("acton_core::actor::actor[new]=error".parse().unwrap())
            .add_directive("acton_core::actor::actor[init]=error".parse().unwrap())
            .add_directive("acton_core::actor::actor[activate]=error".parse().unwrap())
            .add_directive("acton_core::actor::idle=off".parse().unwrap())
            .add_directive("acton_core::actor::idle[act_on_async]=off".parse().unwrap())
            .add_directive("acton_core::actor::idle[act_on]=off".parse().unwrap())
            .add_directive(
                "acton_core::message::outbound_envelope=off"
                    .parse()
                    .unwrap(),
            )
            .add_directive(
                "acton_core::message::broadcast_envelope=off"
                    .parse()
                    .unwrap(),
            )
            .add_directive("acton_core::traits::broker_context=error".parse().unwrap())
            .add_directive("acton_core::traits::actor_context=error".parse().unwrap())
            .add_directive("acton_core::traits::subscribable=error".parse().unwrap())
            .add_directive("supervisor_tests=info".parse().unwrap())
            .add_directive("broker_tests=trace".parse().unwrap())
            .add_directive("launchpad_tests=info".parse().unwrap())
            .add_directive("lifecycle_tests=info".parse().unwrap())
            .add_directive("actor_tests=debug".parse().unwrap())
            .add_directive("load_balancer_tests=info".parse().unwrap())
            .add_directive(
                "acton::tests::setup::actor::pool_item=info"
                    .parse()
                    .unwrap(),
            )
            .add_directive("messaging_tests=info".parse().unwrap());
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
