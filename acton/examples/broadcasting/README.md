# Broadcasting Example

This example demonstrates how multiple agents can communicate using a central broadcaster in the Acton framework. Instead of sending messages directly to each other, agents can broadcast messages, allowing any subscribed agents to react accordingly. This helps decouple the agents from one another, enabling a more flexible and maintainable messaging flow.

In this example, we have three agents:
- **DataCollector**: Collects numerical data.
- **Aggregator**: Maintains a running total of the data collected.
- **Printer**: Responsible for all screen output.

## Key Features Demonstrated
- **Broadcasting Messages**: Agents use a central broadcaster to send and receive messages, enabling multiple agents to react to the same events without direct connections.
- **Agent Subscriptions**: Agents subscribe to messages they're interested in, allowing them to receive only relevant broadcasts.
- **Decoupled Communication**: The example showcases how agents remain loosely coupled by using the broadcaster instead of direct communication.

## How It Works
- The `DataCollector` agent collects incoming numerical data and broadcasts each data point.
- The `Aggregator` agent listens for data points and maintains a running total, broadcasting updates whenever new data arrives.
- The `Printer` agent listens for any messages that need to be displayed on the screen and outputs them accordingly.

## Running the Example
To run the example:
1. Start the Acton application and launch all agents.
2. Send data messages to the `DataCollector`.
3. Watch as the `Aggregator` updates the sum and sends status updates, all displayed by the `Printer`.

## Code Walkthrough

### Initializing the App and Agents
```rust
// Launch the app
let mut app = ActonApp::launch();

// Initialize each agent
let mut data_collector = app.initialize::<DataCollector>().await;
let mut aggregator = app.initialize::<Aggregator>().await;
let mut printer = app.initialize::<Printer>().await;
```
We launch the application and initialize our agents. Each agent is then configured with its behavior.

### DataCollector Agent
The `DataCollector` receives new data and broadcasts it:
```rust
data_collector
    .act_on::<NewData>(|agent, envelope| {
        agent.model.data_points.push(envelope.message().0);
        
        let broker = agent.broker().clone();
        let message = format!("DataCollector received new data: {}", envelope.message().0.clone());
        AgentReply::from_async(async move { broker.broadcast(PrintMessage(message)).await })
    })
    .after_start(|agent| {
        let broker = agent.broker().clone();
        AgentReply::from_async(async move {
            broker.broadcast(PrintMessage("DataCollector is ready to collect data!".to_string())).await;
        })
    });
```

### Aggregator Agent
The `Aggregator` maintains a running total of all data received:
```rust
aggregator
    .act_on::<NewData>(|agent, envelope| {
        agent.model.sum += envelope.message().0;
        
        let broker = agent.broker().clone();
        let sum = agent.model.sum;
        let message = format!("Aggregator updated sum: {}", sum);
        
        AgentReply::from_async(async move {
            broker.broadcast(PrintMessage(message)).await;
        })
    })
    .after_start(|agent| {
        let broker = agent.broker().clone();
        AgentReply::from_async(async move {
            broker.broadcast(PrintMessage("Aggregator is ready to sum data!".to_string())).await;
        })
    })
    .before_stop(|agent| {
        let broker = agent.broker().clone();
        let sum = agent.model.sum;
        AgentReply::from_async(async move {
            broker.broadcast(PrintMessage(format!("Final sum: {sum}"))).await;
        })
    });
```

### Printer Agent
The `Printer` handles all output:
```rust
printer
    .act_on::<PrintMessage>(|_agent, envelope| {
        println!("Printer received: {}", envelope.message().0);
        AgentReply::immediate()
    })
    .after_start(|agent| {
        let broker = agent.broker().clone();
        AgentReply::from_async(async move {
            broker.broadcast(PrintMessage("Printer is ready to display messages!".to_string())).await;
        })
    });
```

### Subscribing to Messages
Agents subscribe to the messages they're interested in:
```rust
data_collector.handle().subscribe::<NewData>().await;
aggregator.handle().subscribe::<NewData>().await;
printer.handle().subscribe::<PrintMessage>().await;
```

### Sending Messages via the Broadcaster
Finally, messages are broadcasted:
```rust
broker.broadcast(NewData(5)).await;
broker.broadcast(NewData(10)).await;

// Demonstrate sending a direct message to the Printer
printer_handle.send_message(PrintMessage("Printing is fun!".to_string())).await;
```

## Running the Example
Run this example with:
```bash
cargo run --example broadcasting
```

You'll see the `Printer` displaying messages as they're broadcast by other agents.

