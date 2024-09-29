# Acton Example: Start Here

Welcome to the **Acton** framework! This "Start Here" example introduces you to creating a simple, concurrent agent that manages a list of items. It highlights Acton's core features: creating an agent, handling messages, and performing asynchronous operations without needing locks or atomic references.

## What This Example Covers

- Creating and initializing an agent
- Defining messages for agent interaction
- Configuring agent lifecycle events
- Handling both synchronous and asynchronous messages
- Demonstrating Acton's concurrency model

## Running the Example

Clone Acton, then run:

```bash
cargo run --example start-here --package acton
```

## Key Concepts Illustrated

1. Agent Model and State Management

   This example defines an ItemTracker struct to manage the agent's state. The ItemTracker contains a Vec<String> for tracking items, showing how to manage state without locks or atomic references:

   ```rust
   #[derive(Default, Debug)]
   struct ItemTracker {
       items: Vec<String>,
   }
   ```

2. Messages

   Messages facilitate interaction with agents. This example defines two messages:
   - `AddItem(String)`: Adds an item to the list.
   - `GetItems`: Retrieves the current list of items.

3. Initializing and Configuring an Agent

   Initialize the agent with `app.initialize::<ItemTracker>()`. Then, configure its behavior using event handlers:

   ```rust
   agent
       .before_start(|_agent| {
           println!("Agent is preparing to track items... Here we go!");
           AgentReply::immediate()
       })
       .after_start(|_agent| {
           println!("Agent is now tracking items!");
           AgentReply::immediate()
       })
   ```

4. Handling Messages

Messages are processed with `act_on`. The agent handles different types of messages:

- Adding an item:

  ```rust
  .act_on::<AddItem>(|agent, envelope| {
      println!("Adding item: {}", envelope.message.0);
      agent.model.items.push(envelope.message.0.clone());
      AgentReply::immediate()
  })
  ```

- Fetching items asynchronously:

  ```rust
  .act_on::<GetItems>(|agent, _envelope| {
      println!("Fetching items... please wait!");
      let items = agent.model.items.clone();
      AgentReply::from_async(async move {
          sleep(Duration::from_secs(2)).await;
          println!("Current items: {:?}", items);
      })
  })
  ```

5. Sending Messages

   You can send messages to the agent using `agent_handle` once it's started:

   ```rust
   agent_handle.send_message(AddItem("Apple".to_string())).await;
   agent_handle.send_message(GetItems).await;
   ```

   The `agent_handle` is cloneable, allowing concurrent message sending from different parts of your application.

6. Graceful Shutdown

   To shut down the system and all agents, use:

   ```rust
   app.shutdown_all().await.expect("Failed to shut down system");
   ```

## Summary

This example introduces Acton's key concepts:
- Agents managing state serially without locks or atomics
- Message definition and handling
- Asynchronous agent actions
- Clean agent lifecycle management

You're now ready to explore more examples or start building concurrent Rust applications with Acton!