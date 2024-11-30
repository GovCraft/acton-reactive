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

// Basic Example: A friendly counter that responds to messages
//
// This example shows the fundamental concepts of agents:
// - Creating an agent with some state
// - Sending messages between agents
// - Handling different types of messages
// - Updating agent state

// An agent is like a worker that can receive messages and maintain its own state
#[derive(Debug, Default)]
struct CounterAgent {
    // Agents can keep track of their own data
    count: usize,
}

// Messages are how we communicate with agents
// Think of them like friendly notes we pass around

// We can use derive macros to set up our messages
#[derive(Debug, Clone)]
struct HelloMsg;

// The #[acton_message] macro makes message setup easier
#[acton_message]
struct ThankYouMsg;

#[acton_message]
struct GoodbyeMsg;

#[tokio::main]
async fn main() {
    // Start up our application
    let mut app = ActonApp::launch();

    // Create a new counter agent
    let mut counter = app.new_agent::<CounterAgent>().await;

    // Tell our agent how to handle different messages
    counter
        // When we receive a Hello message...
        .act_on::<HelloMsg>(|agent, context| {
            println!("👋 Someone said hello! Counting up...");
            agent.model.count += 1;

            // We can reply back to whoever sent us the message
            let envelope = context.reply_envelope();

            // Handlers need to return a pinned future (for async operations)
            Box::pin(async move {
                envelope.send(ThankYouMsg).await;
            })
        })
        // When we receive a Thank You message...
        .act_on::<ThankYouMsg>(|agent, _envelope| {
            println!("🙏 Got a thank you! Counting up again...");
            agent.model.count += 1;

            // We can send a message to ourselves using our own handle
            let handle = agent.handle().clone();

            // This helper makes it easier to return a future
            AgentReply::from_async(async move {
                handle.send(GoodbyeMsg).await;
            })
        })
        // When we receive a Goodbye message...
        .act_on::<GoodbyeMsg>(|agent, _envelope| {
            println!("👋 Time to say goodbye!");

            // When we have no async work, we can return immediately
            AgentReply::immediate()
        })
        // This runs when the agent stops
        .after_stop(|agent| {
            println!("Final count: {}", agent.model.count);
            debug_assert_eq!(agent.model.count, 2);
            AgentReply::immediate()
        });

    // Start our counter agent
    let counter = counter.start().await;

    // Send it a hello message to kick things off!
    counter.send(HelloMsg).await;

    // shutdown tries to gracefully stop all agents and their children
    app.shutdown_all()
        .await
        .expect("Failed to shut down system");
}
