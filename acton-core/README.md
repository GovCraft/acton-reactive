# Acton Core

`acton-core` provides the foundational functionality and abstractions that power the [Acton Reactive Application Framework](https://github.com/Govcraft/acton-reactive). This crate includes essential building blocks for creating reactive, event-driven, and distributed systems in Rust.

## Key Features

- **Core Abstractions**: `acton-core` includes the key components that support message passing, agent lifecycle management, and system orchestration within the Acton framework.
- **Concurrency**: Leveraging Rust’s async capabilities and Tokio, `acton-core` provides a highly concurrent and efficient runtime for agent-based applications.
- **Foundation for Acton**: This crate underpins much of the functionality in [Acton Reactive](https://github.com/Govcraft/acton-reactive) and is not intended to be used directly by developers.

## Usage

You do not need to interact with `acton-core` directly. Instead, use the [Acton Reactive Application Framework](https://github.com/Govcraft/acton-reactive), which re-exports the necessary components from `acton-core` to build reactive, scalable applications.

If you're looking to build reactive applications, please refer to the [Acton Reactive](https://github.com/Govcraft/acton-reactive) repository for more information.

## Learn More

For more details on how the Acton framework works and examples of how to build reactive applications, refer to the [Acton Reactive Application Framework documentation](https://github.com/Govcraft/acton-reactive/blob/main/README.md).

## License

This project is licensed under both the MIT and Apache-2.0 licenses. See the `LICENSE-MIT` and `LICENSE-APACHE` files for details.

