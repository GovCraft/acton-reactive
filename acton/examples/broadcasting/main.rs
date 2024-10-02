use std::io::{stdout, Write};
use std::io::Stdout;

use crossterm::{
    execute,
    style::{Color, Print, ResetColor, SetForegroundColor},
};

use acton::prelude::*;

// Define the data model for each agent
#[derive(Default, Debug)]
struct DataCollector {
    data_points: Vec<i32>,
}

#[derive(Default, Debug)]
struct Aggregator {
    sum: i32,
}

#[derive(Debug)]
struct Printer {
    out: Stdout,
}

impl Default for Printer {
    fn default() -> Self {
        Self {
            out: stdout()
        }
    }
}

// Define messages
#[derive(Clone, Debug)]
struct NewData(i32);

type From = String;
type Status = String;

#[derive(Clone, Debug)]
enum StatusUpdate {
    Ready(From, Status),
    Updated(From, i32),
    Done(i32),
}

#[tokio::main]
async fn main() {
    // Launch the app (required)
    let mut app = ActonApp::launch();

    // Initialize each agent
    let mut data_collector = app.new_agent::<DataCollector>().await;
    let mut aggregator = app.new_agent::<Aggregator>().await;
    let mut printer = app.new_agent::<Printer>().await;

    // Set up the DataCollector agent
    data_collector
        .act_on::<NewData>(|agent, envelope| {
            agent.model.data_points.push(envelope.message().0);

            // Send a message to the Printer via the broker
            let broker = agent.broker().clone();
            let message = envelope.message().0.clone();
            Box::pin(async move { broker.broadcast(StatusUpdate::Updated("DataCollector".to_string(), message)).await })
        })
        .after_start(|agent| {
            let broker = agent.broker().clone();

            Box::pin(async move {
                broker.broadcast(StatusUpdate::Ready("DataCollector".to_string(), "ready to collect data".to_string())).await;
            })
        });

    // Set up the Aggregator agent
    aggregator
        .act_on::<NewData>(|agent, envelope| {
            agent.model.sum += envelope.message().0;

            // Send a message to the Printer via the broker
            let broker = agent.broker().clone();
            let sum = agent.model.sum;
            let message = format!("Aggregator updated sum: {}", sum);

            // Send the aggregated result to the broker using an async operation
            Box::pin(async move { broker.broadcast(StatusUpdate::Updated("Aggregator".to_string(), sum)).await })
        })
        .after_start(|agent| {
            let broker = agent.broker().clone();
            Box::pin(async move {
                broker.broadcast(StatusUpdate::Ready("Aggregator".to_string(), "ready to sum data".to_string())).await;
            })
        })
        .before_stop(|agent| {
            let broker = agent.broker().clone();
            let sum = agent.model.sum;
            Box::pin(async move {
                broker.broadcast(StatusUpdate::Done(sum)).await;
            })
        });

    // Set up the Printer agent
    printer
        .act_on::<StatusUpdate>(|agent, envelope| {
            match &envelope.message() {
                StatusUpdate::Ready(who, what) => {
                    let _ = execute!(
                agent.model.out, //out is a shared IO resource
                SetForegroundColor(Color::DarkYellow),        // Set text color
                Print("\u{2713}  "),  // Print message
                SetForegroundColor(Color::Yellow),         // Set text color
                Print(format!("{}", who)),  // Print message
                SetForegroundColor(Color::DarkYellow),         // Set text color
                Print(format!(" is {}!\n", what)),  // Print message
                ResetColor                             // Reset colors back to default
            );
                }
                StatusUpdate::Updated(who, what) => {
                    let _ = execute!(
                agent.model.out, //out is a shared IO resource
                SetForegroundColor(Color::DarkCyan),        // Set text color
                Print("\u{2139}  "),  // Print message
                SetForegroundColor(Color::Cyan),         // Set text color
                Print(format!("{}", who)),  // Print message
                SetForegroundColor(Color::DarkCyan),         // Set text color
                Print(format!(" is {}!\n", what)),  // Print message
                ResetColor                             // Reset colors back to default
            );
                }
                StatusUpdate::Done(sum) => {
                    let _ = execute!(
                agent.model.out, //out is a shared IO resource
                SetForegroundColor(Color::DarkMagenta),        // Set text color
                Print("\u{1F680} "),  // Print message
                SetForegroundColor(Color::Red),         // Set text color
                Print(format!("Final sum {}!\n\n", sum)),  // Print message
                ResetColor                             // Reset colors back to default
            );
                }
            }

            AgentReply::immediate()
        })
        .after_start(|agent| {
            // why send a message instead of using the agent directly?
            // Because the agent in lifecycle hooks is not mutable
            // and writing to the out field would require mutability in this case
            let broker = agent.broker().clone();
            Box::pin(async move { broker.broadcast(StatusUpdate::Ready("Printer".to_string(), "ready to display messages".to_string())).await })
        });

    // Subscribe agents to broker messages
    data_collector.handle().subscribe::<NewData>().await;
    aggregator.handle().subscribe::<NewData>().await;
    printer.handle().subscribe::<StatusUpdate>().await;

    // Start the agents
    let _data_collector_handle = data_collector.start().await;
    let _aggregator_handle = aggregator.start().await;
    let _printer_handle = printer.start().await;

    // Access the broker
    let broker = app.broker();

    // Send messages via broker using BrokerRequest
    broker.broadcast(NewData(5)).await;
    broker.broadcast(NewData(10)).await;

    // Shut down the system and all agents
    app.shutdown_all().await.expect("Failed to shutdown system");
}
