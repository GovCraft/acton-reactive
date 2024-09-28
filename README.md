# Acton Distributed Actor Framework

The Acton Distributed Actor Framework is a powerful and flexible Rust-based framework designed to build scalable and
efficient distributed systems. This framework leverages the actor model, allowing developers to create robust and
resilient applications with ease.

## Features

- **Actor-based Architecture**: Utilizes the actor model to encapsulate state and behavior, providing a natural
  concurrency model.
- **Hierarchical Actor Management**: Supports hierarchical relationships between actors, enabling efficient resource
  management and security.
- **Asynchronous Messaging**: Leverages Rust's async/await syntax for high-performance, non-blocking communication.
- **Extensibility**: Easily extensible to accommodate various use cases and integrate with existing systems.

## Getting Started

To get started with the Acton framework, add the following to your `Cargo.toml`:

```toml
[dependencies]
acton = { path = "./acton" }
```

## Example Usage

### Creating and Managing Actors

```rust
use acton::prelude::*;

#[derive(Default, Debug)]
struct MyActor;

impl Actor for MyActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        println!("Actor started!");
    }
}

fn main() {
    let system = System::new("example");
    let actor = system.actor_of::<MyActor>("my-actor").unwrap();
    actor.tell(MyMessage, None);
    system.run();
}
```


