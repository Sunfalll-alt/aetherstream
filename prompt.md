Role: Principal Systems & Distributed Systems Engineer specializing in Rust.

Objective:
Guide me through designing and implementing from scratch a production-grade, distributed event-streaming engine written in Rust (working code-name: "AetherStream" / "HydraLog"), serving as a lightweight, memory-safe, zero-JVM alternative to Apache Kafka / Redpanda.

Key Technical Philosophy:
- High throughput, low latency, predictable tail latencies (p99).
- Zero-copy I/O wherever possible (using Linux sendfile/splice or io_uring).
- Drop-in wire protocol compatibility for core Apache Kafka APIs (Produce, Fetch, Metadata, ApiVersions).
- Production-grade Rust idioms: strict error handling (thiserror/anyhow), lock-free or low-contention concurrency primitives, RAII, comprehensive docstrings, modular crate structure (Cargo workspace).

---

### System Architecture Requirements:

1. Storage Engine (Append-Only Commit Log):
   - Partition log architecture: directory per topic partition, segmented files (`.log` and sparse index `.index` / `.timeindex`).
   - Binary format: CRC32 checksum, record length, relative offset, timestamp, headers, key, payload.
   - Indexing: memory-mapped index (`memmap2`) with binary search mapping logical offsets to physical byte offsets in log segments.
   - Log rolling & retention: rolling by segment size (e.g. 1GB) and duration; retention by time and total partition size.
   - Flush policy: configurable OS page cache buffering vs. explicit fsync (fdatasync).

2. Network & Protocol Layer:
   - Wire protocol: Apache Kafka wire format (binary big-endian frame decoder/encoder using `bytes` and `tokio-util::codec`).
   - Handlers for essential requests:
     * `ApiVersions` (v0-v3)
     * `Metadata` (v0-v7)
     * `Produce` (v3-v8) with ack levels (0, 1, -1/all)
     * `Fetch` (v4-v11) with zero-copy log transmission via `tokio-uring` or OS-level file-to-socket piping.
   - Pipelined and multiplexed TCP connection handling via `tokio`.

3. Distributed Consensus & Clustering (Phase 2):
   - Raft-based consensus per partition (or partition groups) using `openraft` or a custom lightweight Raft implementation.
   - Cluster metadata quorum (similar to Kafka KRaft mode — no external ZooKeeper dependency).
   - High Watermark (HW) and Log End Offset (LEO) tracking across replicas.

4. Observability & Telemetry:
   - Structured logging with `tracing` and `tracing-subscriber`.
   - Prometheus metrics export via `metrics` / `metrics-exporter-prometheus` (latencies, fetch/produce rates, disk usage, active connections).

---

### Step-by-Step Task Breakdown:

Please work with me iteratively. Do NOT dump all code at once. We will proceed phase by phase:

- Phase 1: Storage Layer (Single-node Append-only Log, Segments, Memory-mapped Sparse Index, binary encoding/decoding, integration tests for corruption & recovery).
- Phase 2: Kafka Wire Protocol Parser (Framing, Request/Response header codec, basic ApiVersions & Metadata response generation).
- Phase 3: Single-node Broker (Tokio TCP server integrating Network Layer + Storage Engine, supporting standard Kafka CLI `kafka-console-producer` and `kafka-console-consumer`).
- Phase 4: Zero-Copy Optimizations & Benchmarking (`criterion` harness, measuring throughput (MB/s) and latency distribution).
- Phase 5: Replication & Clustering (Raft consensus integration for multi-broker setup).

---

### Your First Response MUST Include:

1. The initial Cargo Workspace layout (`Cargo.toml` and crate breakdown: e.g., `aether-storage`, `aether-protocol`, `aether-server`, `aether-cli`).
2. Recommended dependency list with specific versions/crates and justifications.
3. Complete implementation of Phase 1 - Step 1:
   - Binary Log Record format definition (structs, serialization/deserialization with CRC validation).
   - Core `Segment` struct skeleton with file handling.
4. Instructions and verification test to prove log append and read correctness locally using `cargo test`.
