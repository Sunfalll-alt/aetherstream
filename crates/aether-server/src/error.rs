//! Server error types.

use thiserror::Error;

use aether_protocol::ProtocolError;
use aether_storage::StorageError;

/// Server and request processing errors.
#[derive(Debug, Error)]
pub enum ServerError {
    /// Underlying TCP or filesystem I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Storage engine error.
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    /// Protocol encoding/decoding error.
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    /// Distributed cluster and consensus error.
    #[error("Cluster error: {0}")]
    Cluster(#[from] aether_cluster::ClusterError),

    /// Requested topic does not exist.
    #[error("Unknown topic: {0}")]
    UnknownTopic(String),

    /// Requested partition index does not exist for topic.
    #[error("Unknown partition {partition} for topic {topic}")]
    UnknownPartition {
        /// Topic name.
        topic: String,
        /// Partition index.
        partition: u32,
    },
}
