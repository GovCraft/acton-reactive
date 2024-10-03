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

use acton_reactive::prelude::*;

// agents must derive these traits
#[derive(Debug, Default)]
struct ABasicAgent {
    some_state: usize,
}

// messages must derive these traits
#[derive(Debug, Clone)]
struct PingMsg;

// or save a few keystrokes and use the handy macro
#[acton_message]
struct PongMsg;

#[acton_message]
struct BuhByeMsg;

#[tokio::main]
async fn main() {
    let mut app = ActonApp::launch();
    let mut the_agent = app.new_agent::<ABasicAgent>().await;

    the_agent
        .act_on::<PingMsg>(|agent, context| {
            println!("Pinged. You can mutate me!");
            agent.model.some_state += 1;
            // you can reply to the sender (in this case, the agent itself)
            let envelope = context.reply_envelope();

            // every handler must return a boxed future
            Box::pin(async move {
                envelope.send(PongMsg).await;
            })
        })
        .act_on::<PongMsg>(|agent, _envelope| {
            println!("I got ponged!");
            agent.model.some_state += 1;
            //if you're replying to the same agent, you can use the agent's handle
            let handle = agent.handle().clone(); // handle clones are cheap and need to be done when moving into the async boundary

            // if you find the box pin syntax confusing, you use this helper function
            AgentReply::from_async(async move {
                handle.send(BuhByeMsg).await;
            })
        })
        .act_on::<BuhByeMsg>(|agent, envelope| {
            println!("Thanks for all the fish! Buh Bye!");
            //if you don't have any async work to do, you can reply with an empty boxed future
            //or just use this function
            AgentReply::immediate()
        })
        .after_stop(|agent| {
            println!("Agent stopped with state value: {}", agent.model.some_state);
            debug_assert_eq!(agent.model.some_state, 2);

            AgentReply::immediate()
        });

    let the_agent = the_agent.start().await;

    the_agent.send(PingMsg).await;

    // shutdown tries to gracefully stop all agents and their children
    app.shutdown_all().await.expect("Failed to shut down system");
}
