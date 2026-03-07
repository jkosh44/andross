# Andross

<p align="center">
  <img src="/andross.png">
</p>

A distributed key-value database.

[![Build Status](https://github.com/jkosh44/andross/actions/workflows/ci.yml/badge.svg)](https://github.com/jkosh44/andross/actions)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## Overview

Andross is a distributed key-value database written in Rust. Nodes form a [Raft](https://raft.github.io/) consensus
cluster, so writes are consistently replicated and remain available through node failures. Storage on each node is
backed by [fjall](https://github.com/fjall-rs/fjall), a log-structured, embeddable key-value storage engine written
in Rust.

The project is a Cargo workspace with two crates:

- **`andross-server`**: the database server. Runs a Raft node, accepts gRPC requests via
  [tonic](https://github.com/hyperium/tonic), and stores the Raft log in fjall.
- **`andross-client`**: a command-line client and Rust library for issuing `insert` and `read` requests to a running 
  cluster.

## Basic Usage

```sh
# start a three-node cluster
cargo run --bin andross-server -- --id 1 --port 42666 --database-path /tmp/node1 \
  --peers 2=localhost:42667 --peers 3=localhost:42668
cargo run --bin andross-server -- --id 2 --port 42667 --database-path /tmp/node2 \
  --peers 1=localhost:42666 --peers 3=localhost:42668
cargo run --bin andross-server -- --id 3 --port 42668 --database-path /tmp/node3 \
  --peers 1=localhost:42666 --peers 2=localhost:42667

# write and read via the client
cargo run --bin andross-client -- insert --key hello --value world
cargo run --bin andross-client -- read --key hello
```

## Getting Started

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) 1.89.0 or later (stable toolchain)

### Build

```sh
git clone https://github.com/jkosh44/andross.git
cd andross
cargo build --release
```

## Running a Cluster

Each server node requires a unique Raft ID, a port to listen on, a path to its database directory, and the addresses of its peers.

```sh
# Node 1
cargo run --bin andross-server -- \
  --id 1 \
  --port 42666 \
  --database-path /tmp/andross/node1 \
  --peers 2=[::1]:42667 \
  --peers 3=[::1]:42668

# Node 2
cargo run --bin andross-server -- \
  --id 2 \
  --port 42667 \
  --database-path /tmp/andross/node2 \
  --peers 1=[::1]:42666 \
  --peers 3=[::1]:42668

# Node 3
cargo run --bin andross-server -- \
  --id 3 \
  --port 42668 \
  --database-path /tmp/andross/node3 \
  --peers 1=[::1]:42666 \
  --peers 2=[::1]:42667
```

## Using the Client

Andross can be accessed either through a CLI or a Rust client library. Both approaches accepts multiple server
addresses for automatic failover, if one node is unreachable or rejects a request, it retries against the others.

### CLI

The CLI connects to one or more server addresses and supports two commands:

```sh
# Insert a key-value pair
cargo run --bin andross-client -- \
  --addrs [::1]:42666 --addrs [::1]:42667 \
  insert --key hello --value world

# Read a value by key
cargo run --bin andross-client -- \
  --addrs [::1]:42666 \
  read --key hello
```

### Rust Library

The `andross-client` crate can also be used directly from Rust.

```rust
use andross_client::Client;
use bytes::Bytes;

#[tokio::main]
async fn main() {
    let uris = ["localhost:42666", "localhost:42667", "localhost:42668"]
        .iter()
        .map(|addr| format!("http://{addr}").parse().expect("invalid URI"));

    let mut client = Client::new(uris).await;

    // Insert a key-value pair
    client
        .insert(Bytes::from("hello"), Bytes::from("world"))
        .await
        .expect("insert failed");

    // Read it back
    let value = client
        .read(Bytes::from("hello"))
        .await
        .expect("read failed");

    println!("VALUE: {}", std::str::from_utf8(&value).unwrap());
}
```
