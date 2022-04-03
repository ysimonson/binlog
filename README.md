# binlog

[![Test](https://github.com/ysimonson/binlog/actions/workflows/rust-test.yml/badge.svg)](https://github.com/ysimonson/binlog/actions/workflows/rust-test.yml)
[![crates.io](https://img.shields.io/crates/v/binlog.svg)](https://crates.io/crates/binlog)
[![API docs](https://docs.rs/binlog/badge.svg)](https://docs.rs/binlog)

A rust library for creating and managing logs of arbitrary binary data.

The underlying storage of logs are pluggable via the implementation of a couple of [traits](https://github.com/ysimonson/binlog/blob/main/src/traits.rs). Binlog includes built-in implementations via sqlite storage, and in-memory-only. Additionally, python bindings allow you to use (a subset of) binlog from python.

## Python bindings

WIP.

## Testing

### Unit tests

Tests can be run via `make test`. This will also be run in CI.

### Benchmarks

WIP.

### Fuzzing

A fuzzer is available, ensuring the the sqlite and in-memory datastores operate identically. Run it via `make fuzz`.

### Checks

Lint and formatting checks can be run via `make check`. Equivalent checks will also run in CI.
