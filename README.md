# AetherStream (HydraLog) ⚡

> **A lightweight, ultra-low latency, distributed event-streaming engine written in Rust with Apache Kafka wire-protocol compatibility.**

[![Rust 2021](https://img.shields.io/badge/Rust-2021_Edition-orange?logo=rust)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/Tests-48%20Passed-brightgreen)](https://github.com/Sunfalll-alt/aetherstream)
[![Clippy](https://img.shields.io/badge/Clippy-0%20Warnings-success)](https://github.com/Sunfalll-alt/aetherstream)
[![Zero-Copy Fetch](https://img.shields.io/badge/Fetch_Throughput-9.18_GiB%2Fs-blueviolet)](https://github.com/Sunfalll-alt/aetherstream)
[![Index Lookup](https://img.shields.io/badge/Index_Lookup-14.3_ns-informational)](https://github.com/Sunfalll-alt/aetherstream)

---

**AetherStream** is an append-only distributed streaming engine engineered from first principles in Rust. Built without the JVM, garbage collection overhead, or complex runtime dependencies, it serves as a lightweight, memory-safe, drop-in alternative to Apache Kafka and Redpanda.

Clients written for Kafka (`kcat`, `librdkafka`, `kafka-python`, Java Kafka Client, Go `confluent-kafka-go`) connect directly to AetherStream without modifications or wrapper proxies.

---

## 🏛️ System Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                        Standard Kafka Clients                          │
│          (kcat, librdkafka, kafka-python, Java Kafka Client)           │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ Kafka Wire Protocol (TCP)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                         AetherStream Broker                            │
│                                                                        │
│   ┌────────────────────────────────────────────────────────────────┐   │
│   │                  Tokio Async TCP Multiplexer                   │   │
│   │                   Framed KafkaCodec (v0 - v11)                 │   │
│   └───────────────────────────────┬────────────────────────────────┘   │
│                                   │                                    │
│                                   ▼                                    │
│   ┌────────────────────────────────────────────────────────────────┐   │
│   │               Request Dispatcher & API Handlers                │   │
│   │         ApiVersions  │  Metadata  │  Produce  │  Fetch         │   │
│   └───────────────────────────────┬────────────────────────────────┘   │
│                                   │                                    │
│                                   ▼                                    │
│   ┌────────────────────────────────────────────────────────────────┐   │
│   │              BrokerState & Partition Router Catalog            │   │
│   └──────────────────┬─────────────────────────────┬───────────────┘   │
│                      │                             │                   │
│                      ▼                             ▼                   │
│   ┌───────────────────────────────────┐ ┌──────────────────────────┐   │
│   │          Storage Engine           │ │   Raft Cluster Engine    │   │
│   │ ┌───────────────────────────────┐ │ │     (aether-cluster)     │   │
│   │ │     PartitionLog Router       │ │ └────────────┬─────────────┘   │
│   │ ├───────────────────────────────┤ │              │                 │
│   │ │ Monotonic Commit Segments     │ │              │ Raft RPC Mesh   │
│   │ │  (.log: pread/write_all_at)   │ │              │ (Async TCP)     │
│   │ ├───────────────────────────────┤ │              │                 │
│   │ │ Sparse Index (memmap2)        │ │              │                 │
│   │ │  (.index: O(log N) lookup)    │ │              │                 │
│   │ └───────────────────────────────┘ │              │                 │
│   └───────────────────────────────────┘              │                 │
└──────────────────────────────────────────────────────┼─────────────────┘
                                                       │
                                                       ▼
                                          ┌──────────────────────────┐
                                          │  Peer Follower Replicas  │
                                          │      (Quorum Mesh)       │
                                          └──────────────────────────┘
```

---

## ✨ Key Architectural Highlights

### 1. Storage Engine (`crates/aether-storage`)
- **Segmented Commit Log**: Monotonic log segment files (`00000000000000000000.log`) written using atomic positional I/O (`pread`/`pwrite` style) without lock contention.
- **Sparse Memory-Mapped Index**: Zero-allocation `.index` files backed by `memmap2`. Fixed 8-byte entries (4-byte relative offset + 4-byte physical byte position) enable fast $O(\log N)$ binary search lookups in **~14.3 nanoseconds**.
- **Crash Recovery & Auto-Rebuild**: Detects torn writes and partial frames during broker startup, truncates log files to the last authoritative valid record boundary, and automatically reconstructs missing or damaged index files from disk.
- **Hardware SIMD CRC32**: Every record is validated with IEEE 802.3 CRC32 accelerated by CPU hardware vector extensions via `crc32fast` (**31 GiB/s**).
- **Partition Lifecycle**: Automatic active segment rolling upon reaching capacity limits, transparent multi-segment reading across boundaries, and retention eviction by time and log size.

### 2. Zero-Copy Networking & Vectorized I/O (`crates/aether-server`)
- **Page-Cache Slicing**: Direct slicing of pre-read buffer bytes via `bytes::Bytes` transferred directly into the socket frame, bypassing intermediate copies and achieving **9.18 GiB/s** fetch throughput.
- **Vectorized Appends (`writev`)**: Batch-produce appends aggregate pre-computed record buffers and commit in a single kernel syscall, achieving **756 MiB/s** write throughput on standard SSD storage.

### 3. Kafka Wire Protocol Engine (`crates/aether-protocol`)
- **Strict Wire Compatibility**: Direct binary compatibility with official Apache Kafka request and response frames (4-byte big-endian framing).
- **APIs Supported**:
  - `ApiVersions` (v0–v3)
  - `Metadata` (v0–v7) — broker topology, dynamic partition discovery, and In-Sync Replicas (ISR).
  - `Produce` (v3–v8) — batch records, dynamic timestamp assignment, and configurable acknowledgement levels (`acks = 0, 1, -1`).
  - `Fetch` (v4–v11) — high watermark bounds, zero-copy record slices, and offset validation.
- **Flexible KIP-482 Serialization**: Implements compact strings, compact arrays, varints, zigzag encoding, and tagged field blocks required by modern Kafka clients.

### 4. Distributed Raft Consensus (`crates/aether-cluster`)
- **Consensus Engine**: Lightweight Raft state machine managing cluster roles (`Leader`, `Follower`, `Candidate`), term leases, and leader elections with anti-split-vote jitter.
- **Quorum Replication Pipeline**: Leader replicates incoming produce batches over asynchronous TCP RPCs to follower brokers. For `acks = -1 / all`, writes are held until verified persisted on a majority quorum ($\lfloor N/2 \rfloor + 1$) without blocking the Tokio reactor.
- **Dynamic Cluster Metadata**: Real-time reporting of cluster leaders, replica assignments, and In-Sync Replicas (ISR).
- **Zero Data Loss Failover**: Automatic detection of leader failure with immediate candidate election and uninterrupted client produce/fetch continuation.

---

## 📊 Benchmark Results

All benchmarks executed via **Criterion** on an AMD/Linux development machine using NVMe SSD storage.

### Micro-Benchmarks (`aether-storage`)

| Metric | Measured Throughput / Latency | Implementation Details |
|---|---|---|
| **Zero-Copy Fetch Streaming** | **9.18 GiB/s** | Zero-copy byte slicing from OS page cache |
| **Record Serialization** | **20.84 GiB/s** | Big-endian binary layout without intermediate allocation |
| **Record Deserialization** | **16.42 GiB/s** | Zero-copy `Bytes` slicing with CRC validation |
| **Hardware SIMD CRC32** | **31.02 GiB/s** | SSE4.2 / AVX-accelerated `crc32fast` |
| **Sparse Index Binary Search** | **14.3 ns** / op | $O(\log N)$ binary search over mmap entries |

### Macro-Benchmarks (`aether-server`)

| Benchmark Scenario | Throughput | Latency Profile |
|---|---|---|
| **End-to-End Produce Throughput** | **756.2 MiB/s** (~750,000 rec/sec) | Batch write + CRC calculation + sparse index update |
| **End-to-End Fetch Latency (p50)** | — | **25.4 µs** |
| **End-to-End Fetch Latency (p95)** | — | **58.1 µs** |
| **End-to-End Fetch Latency (p99)** | — | **106.3 µs** |

---

## 🗂️ Workspace Crates

```
crates/
├── aether-storage/     # Append-only segmented commit logs, memmap2 sparse indexing, SIMD CRC32
├── aether-protocol/    # Kafka wire protocol codecs (v0-v11), KIP-482 flexible fields, headers
├── aether-server/      # Tokio async TCP networking, request dispatcher, connection multiplexer
├── aether-cluster/     # Raft consensus engine, leader election, quorum replication pipeline
└── aether-cli/         # Standalone broker binary daemon and CLI entrypoint
```

---

## 🚀 Quickstart Guide

### 1. Prerequisites
- Rust 1.75+ (2021 Edition)
- Linux (x86_64 or aarch64)

### 2. Building from Source

```bash
# Clone the repository
git clone https://github.com/Sunfalll-alt/aetherstream.git
cd aetherstream

# Build optimized release binaries
cargo build --release

# The compiled binary is available at ./target/release/aether
```

### 3. Running a Single Broker Daemon

```bash
./target/release/aether \
  --host 127.0.0.1 \
  --port 9092 \
  --data-dir /tmp/aether-data \
  --node-id 1
```

### 4. Running a 3-Node Distributed Cluster

Start three brokers in separate terminals with inter-broker Raft clustering enabled:

**Node 1:**
```bash
./target/release/aether \
  --host 127.0.0.1 --port 9091 \
  --data-dir /tmp/aether-node1 \
  --node-id 1 \
  --cluster-bind 127.0.0.1:19091
```

**Node 2:**
```bash
./target/release/aether \
  --host 127.0.0.1 --port 9092 \
  --data-dir /tmp/aether-node2 \
  --node-id 2 \
  --cluster-bind 127.0.0.1:19092
```

**Node 3:**
```bash
./target/release/aether \
  --host 127.0.0.1 --port 9093 \
  --data-dir /tmp/aether-node3 \
  --node-id 3 \
  --cluster-bind 127.0.0.1:19093
```

---

## 🛠️ Interacting with Standard Kafka Tools

Because AetherStream is wire-compatible with Kafka, standard command-line utilities work out of the box.

### Using `kcat` (`kafkacat`)

**Query Cluster Metadata:**
```bash
kcat -b localhost:9092 -L
```
*Output:*
```text
Metadata for all topics (from broker 1: localhost:9092/1):
 1 brokers:
  broker 1 at 127.0.0.1:9092 (controller)
 1 topics:
  topic "cluster-events" with 1 partitions:
    partition 0, leader 1, replicas: 1, isrs: 1
```

**Produce Messages:**
```bash
echo "Hello from Kafka Client!" | kcat -b localhost:9092 -t my-topic -P
```

**Consume Messages:**
```bash
kcat -b localhost:9092 -t my-topic -C -o beginning
```

---

## 🧪 Verification & Testing

The workspace includes a complete test suite covering segment rolling, crash recovery, wire protocol encoding/decoding, zero-copy reads, 3-node Raft consensus, and leader failovers:

```bash
# Run all 48 unit and integration tests
cargo test --workspace

# Enforce zero clippy warnings
cargo clippy --workspace --all-targets -- -D warnings

# Check code formatting
cargo fmt --check

# Run performance benchmarks
cargo bench --workspace
```

---

## 📄 License

Licensed under the [Apache License, Version 2.0](LICENSE).
