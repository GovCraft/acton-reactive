# Acton Reactive Application Framework

The Acton Reactive Application Framework provides an efficient way to build fast, reactive Rust applications. Designed
around an actor-based model, it simplifies concurrency and allows developers to focus on writing scalable, maintainable
code. Acton gets its name from the fact that it "acts on" messages you define.

## Key Features

- **Actor-based Architecture**: Acton uses a Tokio-based actor model, enabling a lock-free and highly concurrent state
  management system. This architecture helps maintain a natural concurrency model and simplifies building complex
  systems.
- **Asynchronous Messaging**: By leveraging Rust's async/await syntax, Acton ensures high-performance, non-blocking
  communication between components. This asynchronous model helps achieve responsive and efficient applications, even
  under heavy load.
- **Extensibility**: The framework is designed to be adaptable, making it easy to extend for various use cases or
  integrate with existing systems. This flexibility allows developers to tailor the framework to fit specific
  requirements without compromising performance.
- **Type-safe Message Handling**: Acton enforces type safety in message passing between actors, providing compile-time
  checks and reducing runtime errors. This feature ensures that the system remains robust and reliable, even as it
  scales.
- **Built-in Instrumentation with Tracing**: Acton integrates with the Tracing crate, offering detailed insight into
  application behavior. This instrumentation makes it easier to monitor performance, debug issues, and gain visibility
  into your application's inner workings.## Getting Started

To get started with the Acton framework, add the following to your `Cargo.toml`:

```toml
[dependencies]
acton = { path = "./acton" }
```

## Example Usage

### Creating and Managing Actors

Here's a simple example of how to create and use actors in the Acton framework:

```rust
use acton::prelude::*;

// Define an actor
#[derive(Default)]
struct Counter {
    count: usize,
}

// Implement the Actor trait
impl Actor for Counter {}

// Define a message
#[derive(Clone)]
struct Increment;

// Implement message handling
#[async_trait]
impl Handler<Increment> for Counter {
    async fn handle(&mut self, _msg: Increment, _ctx: &mut ActorContext) {
        self.count += 1;
        println!("Count: {}", self.count);
    }
}

#[tokio::main]
async fn main() {
    // Create an ActonSystem
    let system = ActonSystem::launch();

    // Create an actor
    let counter = system.spawn_actor(Counter::default()).await.unwrap();

    // Send messages to the actor
    counter.send(Increment).await.unwrap();
    counter.send(Increment).await.unwrap();

    // Wait for a moment to see the results
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}
```

This example demonstrates:

1. Defining an actor (`Counter`)
2. Implementing the `Actor` trait
3. Defining a message (`Increment`)
4. Implementing message handling with `Handler<Increment>`
5. Creating an `ActonSystem`
6. Spawning an actor
7. Sending messages to the actor

For more complex examples and advanced usage, please refer to the tests in the `acton/tests/` directory.

## Contributing

Contributions to the Acton Distributed Actor Framework are welcome! Please feel free to submit issues, fork the
repository and send pull requests!

## License

This project is licensed under [LICENSE NAME]. See the LICENSE file for details.


