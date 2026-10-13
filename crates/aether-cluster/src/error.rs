//! Cluster consensus and replication error definitions.

use thiserror::Error;

/// Cluster and consensus error categories.
#[derive(Debug, Error)]
pub enum ClusterError {
    /// Local storage error.
    #[error("Storage error: {0}")]
    Storage(#[from] aether_storage::StorageError),

    /// Wire protocol error.
    #[error("Protocol error: {0}")]
    Protocol(#[from] aether_protocol::ProtocolError),

    /// Network I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Operation directed to a non-leader node.
    #[error("Not leader for partition (current leader: {leader_id:?})")]
    NotLeader {
        /// Current leader node ID if known.
        leader_id: Option<i32>,
    },

    /// Quorum commit wait timed out.
    #[error("Quorum commit timed out at term {term}, target offset {target_offset}")]
    QuorumTimeout {
        /// Raft election term.
        term: u64,
        /// Target offset that failed to achieve quorum.
        target_offset: u64,
    },

    /// Remote peer node unreachable or disconnected.
    #[error("Peer {peer_id} is unavailable or disconnected")]
    PeerUnavailable {
        /// Node ID of the peer.
        peer_id: i32,
    },

    /// Raft term mismatch or stale term rejected.
    #[error("Stale term: local term {current_term}, incoming term {incoming_term}")]
    StaleTerm {
        /// Current term on local node.
        current_term: u64,
        /// Incoming term from peer.
        incoming_term: u64,
    },

    /// Log inconsistency detected during AppendEntries.
    #[error(
        "Log inconsistent: follower LEO {follower_leo}, leader prev offset {leader_prev_offset}"
    )]
    LogInconsistent {
        /// Follower's local Log End Offset.
        follower_leo: u64,
        /// Leader's expected previous offset.
        leader_prev_offset: u64,
    },

    /// Internal asynchronous communication channel closed unexpectedly.
    #[error("Internal channel closed")]
    ChannelClosed,

    /// General cluster error.
    #[error("Cluster error: {0}")]
    General(String),
}
