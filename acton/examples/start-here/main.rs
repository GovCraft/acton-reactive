use acton::prelude::*;
use tokio::time::{sleep, Duration};

// Define the agent's model to track a list of items.
// This demonstrates that no locks or atomics are needed.
#[derive(Default, Debug)]
struct ItemTracker {
    items: Vec<String>, // Using a Vec to show mutable state without locks
}

// Define messages to interact with the agent.
#[derive(Clone, Debug)]
struct AddItem(String);

#[derive(Clone, Debug)]
struct GetItems;

#[tokio::main]
async fn main() {
    // Launch the app (required)
    let mut app = ActonApp::launch();

    // Create and set up the agent
    let mut agent = app.initialize::<ItemTracker>().await;

    // Configure agent behavior
    agent
        .before_start(|_agent| {
            println!("Agent is preparing to track items... Here we go!");
            AgentReply::immediate()
        })
        .after_start(|_agent| {
            println!("Agent is now tracking items!");
            AgentReply::immediate()
        })
        // Handle adding an item
        .act_on::<AddItem>(|agent, envelope| {
            println!("Adding item: {}", envelope.message.0);
            agent.model.items.push(envelope.message.0.clone());
            AgentReply::immediate()
        })
        // Handle retrieving all items with an asynchronous operation
        .act_on::<GetItems>(|agent, _envelope| {
            println!("Fetching items... please wait!");

            // Perform an async operation (simulate a delay)
            let items = agent.model.items.clone();
            AgentReply::from_async(async move {
                sleep(Duration::from_secs(2)).await; // Simulate an async delay
                println!("Current items: {:?}", items);
            })
        })
        .before_stop(|_agent| {
            println!("Agent is stopping... finishing up!");
            AgentReply::immediate()
        })
        .after_stop(|agent| {
            println!("Agent stopped! Final items: {:?}", agent.model.items);
            AgentReply::immediate()
        });

    // Once the agent starts, you can send messages to it, but you can't modify the agent
    // or its model directly. You can only interact with it through messages.
    let agent_handle = agent.start().await;

    // Send messages to add items
    // The agent handle is cloneable, so you can send messages from multiple sources concurrently,
    // including from other agents or different parts of your application.
    agent_handle.send_message(AddItem("Apple".to_string())).await;
    agent_handle.send_message(AddItem("Banana".to_string())).await;
    agent_handle.send_message(AddItem("Cherry".to_string())).await;

    // Retrieve and display the list of items
    agent_handle.send_message(GetItems).await;

    // Shut down the system and all agents
    app.shutdown_all().await.expect("Failed to shut down system");
}
