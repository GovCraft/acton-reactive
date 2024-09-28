# acton_test

`acton_test` is a Rust crate that provides utilities for testing asynchronous code, particularly focused on improving
the experience of writing and running tests for complex asynchronous systems.

## Problem

Testing asynchronous code in Rust can be challenging, especially when dealing with complex systems that involve multiple
asynchronous operations, potential race conditions, and intricate timing issues. Traditional testing approaches often
fall short in providing a seamless experience for developers working with such systems.

Common issues include:

1. Difficulty in setting up the correct runtime environment for async tests.
2. Lack of proper panic handling in async contexts, leading to unclear test failures.
3. Inconsistent logging and tracing across synchronous and asynchronous parts of tests.
4. Cumbersome boilerplate code required for each async test.

## Solution

`acton_test` addresses these issues by providing a simple, yet powerful procedural macro `#[acton_test]` that can be
applied to async test functions. This macro:

1. Automatically sets up the necessary async runtime for your tests.
2. Implements robust panic handling, ensuring that panics in async code are properly caught and reported.
3. Integrates seamlessly with the `tracing` crate for consistent logging across your entire test.
4. Reduces boilerplate, allowing you to focus on writing your test logic.

## Usage

Add `acton_test` to your `Cargo.toml`:

```toml
[dev-dependencies]
acton_test = "1.0.0"
```

In your unit or integration test files:

```rust
use acton_test::prelude::*;

#[acton_test]
async fn my_async_test() {
    // Your async test code here
}
```

## Features

* **Simplified Async Testing**: Write async tests as if they were synchronous, without worrying about runtime setup.
* **Improved Panic Handling**: Panics in async code are caught and reported clearly, making it easier to diagnose test
  failures.
* **Tracing Integration**: Seamless integration with the `tracing` crate for consistent logging in your tests.
* **Reduced Boilerplate**: No need to manually set up async runtimes or handle panics in each test.

## How It Works

The `#[acton_test]` macro expands your async test function into a synchronous test that:

1. Sets up a Tokio runtime.
2. Configures panic and tracing hooks.
3. Runs your async code within this environment.
4. Handles any panics or errors, reporting them in a clear and consistent manner.

This approach ensures that your async tests run in a controlled environment, with proper error handling and logging,
without requiring you to set this up manually for each test.

## Compatibility

`acton_test` is designed to work with Rust's standard library, Tokio, and the `tracing` crate. It should be compatible
with most async Rust code, regardless of the specific async framework being used.

## Current Usage

While `acton_test` is a standalone crate that can be used for testing any asynchronous Rust code, it is currently being
actively used and developed as part of the Acton Actor Framework, an open-source project being created by Govcraft.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.