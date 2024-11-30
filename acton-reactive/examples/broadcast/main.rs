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

// Broadcast Example: A team of agents working together with shared messages
//
// This example shows how multiple agents can work together:
// - Broadcasting messages to multiple agents
// - Coordinating work between different agents
// - Using a shared message broker for communication
// - Pretty printing with colors for better visibility

use acton_reactive::prelude::*;
use crossterm::{
    execute,
    style::{Color, Print, ResetColor, SetForegroundColor},
};
use std::io::Stdout;
use std::io::{stdout, Write};

// Our team of agents! Each one has a specific job:
// 1. DataCollector: Collects and stores numbers
// 2. Aggregator: Adds up all the numbers
// 3. Printer: Shows what everyone is doing

#[derive(Default, Debug)]
struct DataCollector {
    // Keeps track of all the numbers we've seen
    data_points: Vec<i32>,
}

#[derive(Default, Debug)]
struct Aggregator {
    // Keeps a running total of all numbers
    sum: i32,
}

#[derive(Debug)]
struct Printer {
    // Handles colorful output to the terminal
    out: Stdout,
}

// Default setup for our Printer
impl Default for Printer {
    fn default() -> Self {
        Self { out: stdout() }
    }
}

// Messages are like notes our agents pass around
// NewData: When we get a new number to process
#[derive(Clone, Debug)]
struct NewData(i32);

// Status updates use these types
type From = String; // Who sent the message
type Status = String; // What's the message about

// StatusUpdate: Different kinds of updates agents can share
#[derive(Clone, Debug)]
enum StatusUpdate {
    Ready(From, Status), // "I'm ready to work!"
    Updated(From, i32),  // "I just processed this number!"
    Done(i32),           // "All done! Here's the final result!"
}

#[tokio::main]
async fn main() {
    // Start up our application
    let mut app = ActonApp::launch();

    // Create our team of agents
    let mut data_collector = app.new_agent::<DataCollector>().await;
    let mut aggregator = app.new_agent::<Aggregator>().await;
    let mut printer = app.new_agent::<Printer>().await;

    // Set up the DataCollector agent
    data_collector
        // When we get new data...
        .act_on::<NewData>(|agent, envelope| {
            // Store the new number
            agent.model.data_points.push(envelope.message().0);

            // Let everyone know we got new data
            let broker = agent.broker().clone();
            let message = envelope.message().0;
            Box::pin(async move {
                broker
                    .broadcast(StatusUpdate::Updated("DataCollector".to_string(), message))
                    .await
            })
        })
        // When we start up...
        .after_start(|agent| {
            let broker = agent.broker().clone();
            Box::pin(async move {
                broker
                    .broadcast(StatusUpdate::Ready(
                        "DataCollector".to_string(),
                        "ready to collect data".to_string(),
                    ))
                    .await
            })
        });

    // Set up the Aggregator agent
    aggregator
        // When we get new data...
        .act_on::<NewData>(|agent, envelope| {
            // Add the new number to our sum
            agent.model.sum += envelope.message().0;

            // Let everyone know we updated the sum
            let broker = agent.broker().clone();
            let sum = agent.model.sum;
            Box::pin(async move {
                broker
                    .broadcast(StatusUpdate::Updated("Aggregator".to_string(), sum))
                    .await
            })
        })
        // When we start up...
        .after_start(|agent| {
            let broker = agent.broker().clone();
            Box::pin(async move {
                broker
                    .broadcast(StatusUpdate::Ready(
                        "Aggregator".to_string(),
                        "ready to sum data".to_string(),
                    ))
                    .await
            })
        })
        // When we're shutting down...
        .before_stop(|agent| {
            let broker = agent.broker().clone();
            let sum = agent.model.sum;
            Box::pin(async move { broker.broadcast(StatusUpdate::Done(sum)).await })
        });

    // Set up the Printer agent
    printer
        // When we get a status update...
        .act_on::<StatusUpdate>(|agent, envelope| {
            match &envelope.message() {
                // Someone is ready to work
                StatusUpdate::Ready(who, what) => {
                    let _ = execute!(
                        agent.model.out,
                        SetForegroundColor(Color::DarkYellow),
                        Print("\u{2713}  "), // Checkmark
                        SetForegroundColor(Color::Yellow),
                        Print(format!("{}", who)),
                        SetForegroundColor(Color::DarkYellow),
                        Print(format!(" is {}!\n", what)),
                        ResetColor
                    );
                }
                // Someone processed new data
                StatusUpdate::Updated(who, what) => {
                    let _ = execute!(
                        agent.model.out,
                        SetForegroundColor(Color::DarkCyan),
                        Print("\u{2139}  "), // Info symbol
                        SetForegroundColor(Color::Cyan),
                        Print(format!("{}", who)),
                        SetForegroundColor(Color::DarkCyan),
                        Print(format!(" is {}!\n", what)),
                        ResetColor
                    );
                }
                // We're all done!
                StatusUpdate::Done(sum) => {
                    let _ = execute!(
                        agent.model.out,
                        SetForegroundColor(Color::DarkMagenta),
                        Print("\u{1F680} "), // Rocket
                        SetForegroundColor(Color::Red),
                        Print(format!("Final sum {}!\n\n", sum)),
                        ResetColor
                    );
                }
            }
            AgentReply::immediate()
        })
        // When we start up...
        .after_start(|agent| {
            let broker = agent.broker().clone();
            Box::pin(async move {
                broker
                    .broadcast(StatusUpdate::Ready(
                        "Printer".to_string(),
                        "ready to display messages".to_string(),
                    ))
                    .await
            })
        });

    // Subscribe agents to the messages they care about
    data_collector.handle().subscribe::<NewData>().await;
    aggregator.handle().subscribe::<NewData>().await;
    printer.handle().subscribe::<StatusUpdate>().await;

    // Start all our agents
    let _data_collector_handle = data_collector.start().await;
    let _aggregator_handle = aggregator.start().await;
    let _printer_handle = printer.start().await;

    // Get the message broker so we can send messages
    let broker = app.broker();

    // Send some test numbers through the system
    broker.broadcast(NewData(5)).await;
    broker.broadcast(NewData(10)).await;

    // Shut everything down nicely
    app.shutdown_all().await.expect("Failed to shutdown system");
}
