# AetherStream (HydraLog)

A production-grade, zero-JVM, high-performance distributed event-streaming engine written in Rust, engineered as a lightweight, memory-safe alternative to Apache Kafka and Redpanda.

## Key Design Principles
- **Predictable Latency & High Throughput**: Append-only commit log with sub-nanosecond hardware CRC32 integrity validation.
- **Zero-Copy I/O**: Designed for Linux `sendfile`/`splice` and memory-mapped file access (`memmap2`).
- **Wire Protocol Compatibility**: Drop-in protocol compatibility for Apache Kafka APIs (`Produce`, `Fetch`, `Metadata`, `ApiVersions`).
- **Zero JVM & Low Footprint**: Native compiled binary with deterministic memory allocation and low tail latencies (p99).

## Architecture & Progress

### Phase 1: Single-Node Storage Layer (Completed)
- **Record Framing**: Binary record format with SIMD-accelerated CRC32 IEEE data integrity protection (`crc32fast`), supporting zero-copy payload slicing via `bytes::Bytes`.
- **Segment Management**: Thread-safe monotonic append-only log files (`.log`) with atomic POSIX `pread` operations and crash-resilient startup recovery (truncating incomplete/torn trailing writes).
- **Sparse Indexing**: Memory-mapped sparse index (`.index`) using `memmap2`, enabling $O(\log N)$ binary search lookups from logical offsets to nearest physical byte positions.
- **Index Crash Recovery & Rebuild**: Automatic detection and reconstruction of missing or corrupted `.index` files from authoritative `.log` segments.
- **Partition Log Coordinator (`PartitionLog`)**: Multi-segment directory management (`BTreeMap<u64, Segment>`), automatic active segment rolling at capacity limits, cross-segment contiguous record streaming, and retention eviction policies (size and age).

### Upcoming Phases
- **Phase 2**: Kafka Wire Protocol Parser (Framing, Request/Response codecs, `ApiVersions`, `Metadata`).
- **Phase 3**: Single-Node Broker (Tokio TCP multiplexer, integration with `PartitionLog`).
- **Phase 4**: Zero-Copy Optimizations & Benchmarks (`criterion`).
- **Phase 5**: Distributed Replication & Raft Consensus (KRaft mode).

## Workspace Structure
```
├── crates/
│   ├── aether-storage/     # Append-only commit log, segments, sparse mmap index, partition coordinator
│   ├── aether-protocol/    # Kafka wire protocol codecs and request/response models
│   ├── aether-server/      # Async TCP networking, multiplexing, and broker runtime
│   └── aether-cli/         # Management CLI and server daemon binary
```

## Building and Testing
Ensure you have a modern Rust toolchain (1.75+ or 2021 edition):

```bash
# Run full test suite
cargo test --workspace

# Run linter
cargo clippy --workspace -- -D warnings
```
