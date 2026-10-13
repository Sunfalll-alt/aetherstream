//! AetherStream Distributed Consensus & Clustering Engine.
//!
//! Provides quorum-based leader election, Raft consensus, dynamic In-Sync Replicas (ISR),
//! high watermark tracking, and cross-broker partition replication pipelines.

pub mod consensus;
pub mod engine;
pub mod error;
pub mod peer;
pub mod replication;
pub mod rpc;
pub mod topology;

pub use consensus::{ConsensusState, Role};
pub use engine::{ClusterEngine, PartitionProvider};
pub use error::ClusterError;
pub use peer::PeerClient;
pub use replication::{FollowerProgress, PartitionReplication};
pub use rpc::{
    AppendEntriesRequest, AppendEntriesResponse, ClusterCodec, ClusterMessage, VoteRequest,
    VoteResponse,
};
pub use topology::{ClusterTopology, NodeInfo, PartitionReplicaSet};
