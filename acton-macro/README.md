# Acton Macro

`acton-macro` provides procedural macros that simplify the creation of messages and agents in the [Acton Reactive Application Framework](https://github.com/Govcraft/acton-reactive). These macros reduce boilerplate and make your code more concise, allowing you to focus on building reactive, event-driven applications.

## Key Features

- **Simplified Message Creation**: The `acton_message` macro helps you quickly define messages that are compatible with the Acton framework.
- **Agent Macro Support**: Procedural macros allow for cleaner and more expressive agent definitions.
- **Integration with Acton**: `acton-macro` is designed to be used with the [Acton Reactive Application Framework](https://github.com/Govcraft/acton-reactive), a Rust framework for building reactive and scalable applications using an agent-based model.

## Usage

The macros provided by `acton-macro` are re-exported in the [Acton Reactive Application Framework](https://github.com/Govcraft/acton-reactive), so you don't need to import them separately. When you use `acton-reactive`, the macros like `acton_message` are available for use directly.

For example:

```rust
#[acton_message]
struct MyMessage;
```

This macro automatically generates the necessary traits for messages to be used with agents in the Acton framework.
or
```rust
#[acton_actor]
struct MyActor;
```
This macro automatically generates the necessary traits for structs to be used as agents in the Acton framework.

## Learn More

For more advanced usage and detailed examples of how to use these macros in building reactive systems, refer to the [Acton Reactive Application Framework](https://github.com/Govcraft/acton-reactive) documentation and examples.

## License

This project is licensed under both the MIT and Apache-2.0 licenses. See the `LICENSE-MIT` and `LICENSE-APACHE` files for details.
