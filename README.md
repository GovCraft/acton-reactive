# Acton: A Friendly Reactive Framework for Rust

Welcome to Acton! This framework makes it easy to build fast, responsive Rust
applications using a simple message-passing system. Think of Acton as a team of
helpful workers (I call them "agents") who can pass messages to each other and
handle different tasks independently. The name "Acton" comes from what these
agents do - they "act on" the messages you send them!

## What Makes Acton Special?

- **Easy-to-Use Agents**: Agents are like independent workers in your
  application. Each one can handle its own tasks and keep track of its own
  information, making it natural to write programs that do many things at once.

- **Simple Message Passing**: Agents communicate by sending messages to each
  other, just like people passing notes. Thanks to Rust's async/await features,
  these messages get delivered efficiently without blocking other work.

- **Flexible and Adaptable**: You can easily customize Acton to fit your needs.
  Whether you're building a small application or a large system, Acton grows
  with you.

- **Safe and Reliable**: Rust's type system helps ensure that messages get
  delivered to the right places. This means fewer bugs and more reliable
  applications.

- **Built-in Monitoring**: Acton comes with tools to help you see what's
  happening inside your application, making it easier to find and fix problems.

## Getting Started

Add Acton to your project by putting this in your `Cargo.toml`:

```toml
[dependencies]
acton_reactive = { version = "1.1.0-beta.1" }
```

## How to Use Acton

Let's walk through a simple example to see how Acton works!

### Step 1: Set Up Your Agent

First, let's import what I need and create a simple agent:

```rust
use acton_reactive::prelude::*;

// Create an agent that can keep track of a number
#[derive(Debug, Default)]
struct CounterAgent {
    count: usize,
}
```

### Step 2: Define Your Messages

Messages are how agents communicate. Let's create some simple ones:

```rust
// The traditional way:
#[derive(Debug, Clone)]
struct PingMsg;

// Or use the helpful shortcut:
#[acton_message]
struct PongMsg;

#[acton_message]
struct GoodbyeMsg;
```

### Step 3: Create Your Application

Here's how to set up your application and create an agent:

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Start up Acton
    let mut app = ActonApp::launch();

    // Create the counter agent
    let mut counter = app.new_agent::<CounterAgent>().await;
```

### Step 4: Tell Your Agent How to Handle Messages

Now let's make the agent do something when it receives messages:

```rust
counter
    // When someone sends a Ping...
    .act_on::<PingMsg>(|agent, context| {
        println!("Got a ping! Adding to count...");
        agent.model.count += 1;

        // Send a Pong back
        let envelope = context.reply_envelope();
        Box::pin(async move {
            envelope.send(PongMsg).await;
        })
    })
    // When someone sends a Pong...
    .act_on::<PongMsg>(|agent, _envelope| {
        println!("Got a pong! Adding to count...");
        agent.model.count += 1;

        // Send a Goodbye to ourselves
        let handle = agent.handle().clone();
        AgentReply::from_async(async move {
            handle.send(GoodbyeMsg).await;
        })
    })
    // When someone says Goodbye...
    .act_on::<GoodbyeMsg>(|_agent, _envelope| {
        println!("Time to say goodbye!");
        AgentReply::immediate()
    });
```

### Step 5: Start Your Agent and Send Messages

```rust
    // Start the agent
    let counter = counter.start().await;

    // Send it a message
    counter.send(PingMsg).await;

    // Shut everything down nicely
    app.shutdown_all().await?;
    Ok(())
}
```

Want to see more? Check out the example projects:

- **basic**: A simple example like the one above

  ![Basic example of agents with message passing](https://vhs.charm.sh/vhs-5862rvOfSol8EG8BJ9FljF.gif)

- **lifecycles**: See how agents start up and shut down
  ![Example of handing agent lifecycle events](https://vhs.charm.sh/vhs-6ulmK4rdVygT2FCh2n3r6I.gif)

- **broadcast**: Learn how to send messages to multiple agents
  ![Example of broadcasting messages to multiple agents at once](https://vhs.charm.sh/vhs-2yA1DsMZUyjlurHzg9j2v0.gif)

- **fruit_market**: A fun example showing how to build a more complex system
  ![Example of a more complex system with multiple agents handling different responsibilities](https://vhs.charm.sh/vhs-lfX5VU5zIsQ1Ch8GhLwEc.gif)
  )

## Common Questions

### Why do you call them "agents" instead of "actors"?

While Acton is similar to traditional actor frameworks (like Akka or Erlang), I
use the term "agent" to keep things simple and friendly. An agent is just
something that can receive messages and act on them - no complicated theory
required!

### What exactly is an agent?

An agent in Acton is like a helpful worker that can:

- Keep track of its own information
- Receive and respond to messages
- Work independently of other agents
- Handle tasks without blocking others

Think of agents as team members who can work on their own tasks while
communicating with each other when needed.

### Is Acton complicated to use?

Not at all! While Acton is powerful enough for complex applications, I've
designed it to be easy to understand and use. It takes advantage of Rust's
modern features (like async/await) to keep things simple while still being fast
and reliable.

## Want to Help?

I'd love your help making Acton even better! Feel free to:

- Report issues you find
- Suggest new features
- Send pull requests
- Share how you're using Acton

## License

You can use Acton under either the MIT or Apache-2.0 license - whichever works
better for you. Check out the `LICENSE-MIT` and `LICENSE-APACHE` files for the
details.

## Contact

Find me on [Bluesky](https://bsky.app/profile/govcraft.ai)
