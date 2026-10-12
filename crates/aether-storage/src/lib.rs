//! # AetherStream Storage Engine
//!
//! A production-grade, zero-JVM append-only commit log engine engineered for high-throughput
//! streaming event architectures.
//!
//! ## Core Architecture:
//! - **Record**: Binary log framing with hardware-accelerated CRC32 IEEE data integrity protection.
//! - **Segment**: Monotonic append-only log files (`.log`) with thread-safe atomic `pread`/`pwrite`
//!   POSIX operations and automatic crash-recovery via trailing frame truncation.
//! - **Index**: Memory-mapped sparse index (`memmap2`) for $O(\log N)$ binary search offset-to-physical translations.
//! - **PartitionLog**: Topic-partition commit log coordinator managing segment catalog, automatic rolling,
//!   cross-segment contiguous scans, and age/size retention policies.

#![warn(missing_docs)]

pub mod error;
pub mod index;
pub mod partition;
pub mod record;
pub mod segment;

pub use error::StorageError;
pub use index::{Index, IndexConfig, IndexEntry, ENTRY_SIZE, INDEX_FILE_EXT};
pub use partition::{PartitionConfig, PartitionLog, RetentionReport};
pub use record::{Header, Record, ATTR_NONE, CURRENT_RECORD_MAGIC};
pub use segment::{AppendResult, Segment, SegmentConfig, LOG_FILE_EXT};
