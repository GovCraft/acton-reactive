# Acton Actor Framework

The Acton Actor Framework simplifies writing fast, reactive Rust applications.

## Features

- **Actor-based Architecture**: Uses a Tokio-based actor model to for lock-free state and behavior, providing a natural
  concurrency model.
- **Asynchronous Messaging**: Leverages Rust's async/await syntax for high-performance, non-blocking communication.
- **Extensibility**: Easily extensible to accommodate various use cases and integrate with existing systems.
- **Type-safe Message Handling**: Ensures compile-time correctness of message passing between actors.

## Getting Started

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

Contributions to the Acton Distributed Actor Framework are welcome! Please feel free to submit issues, fork the repository and send pull requests!

## License

This project is licensed under [LICENSE NAME]. See the LICENSE file for details.


