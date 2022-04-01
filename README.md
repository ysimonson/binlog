# binlog

[![Test](https://github.com/ysimonson/binlog/actions/workflows/CI.yml/badge.svg)](https://github.com/ysimonson/binlog/actions/workflows/CI.yml)
[![crates.io](https://img.shields.io/crates/v/binlog.svg)](https://crates.io/crates/binlog)
[![API docs](https://docs.rs/binlog/badge.svg)](https://docs.rs/binlog)

A rust library for creating and managing logs of arbitrary binary data.

## Features

* Pluggable underlying storage, with built-in support for sqlite and in-memory-only storage.
* Python bindings, allowing you to append to the log from python.
* Written in rust! High performance, no GC pauses, and a higher degree of safety.

## Python bindings

WIP.

## Testing

### Unit tests

WIP.

### Benchmarks

WIP.

### Fuzzing

A fuzzer is available, ensuring the the sqlite and in-memory datastores operate identically. Run it via `make fuzz`.

### Checks

Lint and formatting checks can be run via `make check`. Equivalent checks will be run in CI.
