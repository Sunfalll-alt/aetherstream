//! Error types for the AetherStream storage engine.

use thiserror::Error;

/// Storage engine error variants.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Underlying filesystem I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Checksum verification failure indicating bit-rot or write corruption.
    #[error("Record CRC32 mismatch: expected {expected:#010x}, calculated {calculated:#010x}")]
    CrcMismatch {
        /// CRC stored in the record header.
        expected: u32,
        /// CRC calculated over the payload.
        calculated: u32,
    },

    /// Frame or record stream truncated before expected bytes could be read.
    #[error("Incomplete record frame: expected {expected} bytes, available {available}")]
    IncompleteRecord {
        /// Expected byte length.
        expected: usize,
        /// Available byte count.
        available: usize,
    },

    /// Structural or schema validation failure during deserialization.
    #[error("Invalid record format: {0}")]
    InvalidFormat(String),

    /// Attempted to append beyond configured segment file capacity limit.
    #[error("Segment capacity exceeded: current size {current_bytes} bytes, record requires {record_bytes} bytes, limit {max_bytes} bytes")]
    SegmentFull {
        /// Current segment size in bytes.
        current_bytes: u64,
        /// Size of the record attempting to append.
        record_bytes: usize,
        /// Configured segment maximum limit in bytes.
        max_bytes: u64,
    },

    /// Requested logical offset is outside the range managed by this segment.
    #[error("Offset out of bounds: requested {requested}, segment base {base_offset}, next offset {next_offset}")]
    OffsetOutOfBounds {
        /// Logical offset requested.
        requested: u64,
        /// Starting base offset of the segment.
        base_offset: u64,
        /// Next append offset of the segment.
        next_offset: u64,
    },

    /// Requested logical offset is outside the range managed by the partition log.
    #[error("Partition offset out of bounds: requested {requested}, start offset {start_offset}, end offset (LEO) {end_offset}")]
    PartitionOffsetOutOfBounds {
        /// Logical offset requested.
        requested: u64,
        /// Starting offset of the partition log.
        start_offset: u64,
        /// Log end offset (next unwritten offset).
        end_offset: u64,
    },

    /// Corrupted record encountered during recovery.
    #[error("Corrupted record encountered at physical position {physical_position}: {reason}")]
    CorruptedRecord {
        /// Byte offset in the segment file.
        physical_position: u64,
        /// Reason description.
        reason: String,
    },

    /// Sparse index file corruption or inconsistency with log segment.
    #[error("Index corruption: {0}")]
    IndexCorrupted(String),

    /// Sparse index reached maximum entry capacity.
    #[error("Index capacity exceeded: current entries {entries}, limit {max_entries}")]
    IndexFull {
        /// Number of entries currently written.
        entries: usize,
        /// Maximum allowed entries.
        max_entries: usize,
    },
}
