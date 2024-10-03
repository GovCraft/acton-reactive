use tokio::time::{sleep, Duration};
use acton_reactive::prelude::*;

#[derive(Default, Debug)]
struct ItemTracker {
    items: Vec<String>,
}

#[derive(Clone, Debug)]
struct AddItem(String);

#[derive(Clone, Debug)]
struct GetItems;

#[tokio::main]
async fn main() {
    let mut app = ActonApp::launch();
    let mut tracker_agent = app.new_agent::<ItemTracker>().await;

    tracker_agent
        .before_start(|_| {
            println!("Agent is preparing to track items... Here we go!");
            AgentReply::immediate()
        })
        .after_start(|_| {
            println!("Agent is now tracking items!");
            AgentReply::immediate()
        })
        .act_on::<AddItem>(|agent, envelope| {
            let item = &envelope.message().0;
            println!("Adding item: {}", item);
            agent.model.items.push(item.clone());
            AgentReply::immediate()
        })
        .act_on::<GetItems>(|agent, _| {
            println!("Fetching items... please wait!");
            let items = agent.model.items.clone();
            AgentReply::from_async(async move {
                sleep(Duration::from_secs(2)).await;
                println!("Current items: {:?}", items);
            })
        })
        .before_stop(|_| {
            println!("Agent is stopping... finishing up!");
            AgentReply::immediate()
        })
        .after_stop(|agent| {
            println!("Agent stopped! Final items: {:?}", agent.model.items);
            AgentReply::immediate()
        });

    let tracker_handle = tracker_agent.start().await;

    tracker_handle.send(AddItem("Apple".to_string())).await;
    tracker_handle.send(AddItem("Banana".to_string())).await;
    tracker_handle.send(AddItem("Cherry".to_string())).await;

    tracker_handle.send(GetItems).await;

    app.shutdown_all().await.expect("Failed to shut down system");
}
