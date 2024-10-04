# Acton Reactive Application Framework

The Acton Reactive Application Framework provides an efficient way to build fast, reactive Rust applications. Designed around an agent-based model, it simplifies concurrency and allows developers to focus on writing scalable, maintainable code. Acton gets its name from the fact that it "acts on" messages you define.

## Key Features

- **Agent-based Architecture**: Acton uses a Tokio-based agent model, enabling a lock-free and highly concurrent state management system. This architecture helps maintain a natural concurrency model and simplifies building complex systems.
- **Asynchronous Messaging**: By leveraging Rust's async/await syntax, Acton ensures high-performance, non-blocking communication between components. This asynchronous model helps achieve responsive and efficient applications, even under heavy load.
- **Extensibility**: The framework is designed to be adaptable, making it easy to extend for various use cases or integrate with existing systems. This flexibility allows developers to tailor the framework to fit specific requirements without compromising performance.
- **Type-safe Message Handling**: Acton enforces type safety in message passing between agents, providing compile-time checks and reducing runtime errors. This feature ensures that the system remains robust and reliable, even as it scales.
- **Built-in Instrumentation with Tracing**: Acton integrates with the Tracing crate, offering detailed insight into application behavior. This instrumentation makes it easier to monitor performance, debug issues, and gain visibility into your application's inner workings.

## Getting Started

To get started with the Acton framework, add the following to your `Cargo.toml`:

```toml
[dependencies]
acton_reactive = { version = "1.1.0-beta.1" }
```

## Example Usage

### Creating and Managing Agents

Here's a simple example of how to create and use agents in the Acton framework:
- Import the prelude.
```rust
use acton_reactive::prelude::*;
```
- An agent can be any struct that implements the `Default` and `Debug` traits. Here's an example of a basic agent:
```rust
// agents must derive these traits
#[derive(Debug, Default)]
struct ABasicAgent {
    some_state: usize,
}
```
- Define messages that the agent can act on. Messages must implement the `Debug` and `Clone` traits. Here's an example:
```rust
// messages must derive these traits
#[derive(Debug, Clone)]
struct PingMsg;
```
- Here's another example of messages where the required traits are added using the `acton_message` macro:
```rust

// or save a few keystrokes and use the handy macro
#[acton_message]
struct PongMsg;

#[acton_message]
struct BuhByeMsg;
```
- Acton uses tokio and so must be operating under an existing tokio runtime. The `ActonApp` struct is used to launch the Acton system. Here's an example of launching the Acton system and creating an agent:
```rust

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut app = ActonApp::launch();
    let mut the_agent = app.new_agent::<ABasicAgent>().await;
```
- Define message handling logic for the agent before the agent is started. 
Here's an example of handling messages and replying to them:
```rust

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
```
    Handlers can be chained together to handle multiple message types. Here's an example of chaining handlers:
```rust

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
```
- Agents have lifecycle hooks that can be used to perform actions before or after the agent starts or stops. Here's an example of using lifecycle hooks:
```rust

    .after_stop(|agent| {
            println!("Agent stopped with state value: {}", agent.model.some_state);
            debug_assert_eq!(agent.model.some_state, 2);

            AgentReply::immediate()
        });
```
- Start your agent to spawn listening and get a handle to send messages to it:
```rust

    let the_agent = the_agent.start().await;

    the_agent.send(PingMsg).await;
```
- Finally, shut down the Acton system:
```rust

    // shutdown tries to gracefully stop all agents and their children
    app.shutdown_all().await?;
    Ok(())
}
```
You can view this example and run it from the examples `basic` folder.

For more advanced usage, check out the **lifecycles**, **broadcast**, and **fruit_market** examples, which demonstrate other messaging patterns and system-wide coordination across agents.

# FAQ
## Why is this called an "agent-based" framework and not an "actor-based" one?
While Acton is, at its core, an actor framework, I’ve chosen to use the term "agent" to focus on clarity and accessibility. The word "actor" comes with a lot of technical baggage from traditional actor models like Akka and Erlang, which may seem complex or intimidating to some developers.

The term "agent" emphasizes the framework’s core principle: it acts on messages in a straightforward, scalable way. By avoiding the technical connotations of "actor," I hope to make Acton more approachable and easier to understand, especially for those who may be new to concurrency patterns. In essence, agents in Acton do the same things that actors do in other systems—handle state, process messages, and operate concurrently—but with a focus on flexibility and simplicity.

## How does an agent differ from an actor in this framework?
Functionally, agents in Acton perform the same role as actors in other frameworks. They:

Maintain independent state.
Process incoming messages asynchronously.
Operate in a concurrent, non-blocking environment.
The difference is mainly in terminology. I want to make Acton more accessible to developers who may not be familiar with the traditional actor model. The term "agent" avoids preconceptions and focuses on what matters: acting on messages efficiently, without introducing unnecessary complexity.

## Is Acton a traditional actor framework?
Acton takes inspiration from traditional actor models, but it’s designed to be more flexible and user-friendly. It still provides the core benefits of actor-based concurrency—message passing, state isolation, and non-blocking processing—while also leveraging modern Rust features like async/await to ensure performance and safety.

In short, Acton is an actor framework by design but optimized for modern Rust applications and with a focus on practicality rather than adhering to a strict actor model.

## Contributing

Contributions to the Acton Reactive Application Framework are welcome! Please feel free to submit issues, fork the repository, and send pull requests!

## License

This project is licensed under both the MIT and Apache-2.0 licenses. See the `LICENSE-MIT` and `LICENSE-APACHE` files for details.
