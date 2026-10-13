//! Cluster node topology and replica set membership.

use std::collections::HashMap;
use std::net::SocketAddr;

use aether_protocol::{BrokerMetadata, ErrorCode, PartitionMetadata};

/// Information about a cluster broker node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    /// Broker node ID.
    pub node_id: i32,
    /// Client-facing Kafka wire protocol address.
    pub client_addr: SocketAddr,
    /// Inter-broker Raft cluster replication address.
    pub peer_addr: SocketAddr,
}

/// Partition replica assignment and In-Sync Replicas (ISR) state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReplicaSet {
    /// Active leader broker node ID.
    pub leader_id: Option<i32>,
    /// Active leader epoch / Raft term.
    pub leader_epoch: u64,
    /// All assigned replica broker node IDs.
    pub replicas: Vec<i32>,
    /// In-Sync Replicas (ISR) subset.
    pub isr: Vec<i32>,
    /// High Watermark offset (committed across quorum).
    pub high_watermark: u64,
    /// Leader's local Log End Offset.
    pub log_end_offset: u64,
}

impl PartitionReplicaSet {
    /// Creates a new replica set with specified leader and replicas.
    pub fn new(leader_id: Option<i32>, replicas: Vec<i32>) -> Self {
        let isr = replicas.clone();
        Self {
            leader_id,
            leader_epoch: 0,
            replicas,
            isr,
            high_watermark: 0,
            log_end_offset: 0,
        }
    }
}

/// Cluster-wide topology view.
#[derive(Debug, Clone)]
pub struct ClusterTopology {
    /// Local broker node ID.
    pub local_node_id: i32,
    /// All nodes in the cluster.
    pub nodes: HashMap<i32, NodeInfo>,
    /// Partition replica sets indexed by (topic, partition_id).
    pub partitions: HashMap<(String, u32), PartitionReplicaSet>,
}

impl ClusterTopology {
    /// Creates a new topology containing the local node.
    pub fn new(local_node_id: i32, local_client: SocketAddr, local_peer: SocketAddr) -> Self {
        let mut nodes = HashMap::new();
        nodes.insert(
            local_node_id,
            NodeInfo {
                node_id: local_node_id,
                client_addr: local_client,
                peer_addr: local_peer,
            },
        );

        Self {
            local_node_id,
            nodes,
            partitions: HashMap::new(),
        }
    }

    /// Adds a peer broker node to the cluster topology.
    pub fn add_node(&mut self, node_id: i32, client_addr: SocketAddr, peer_addr: SocketAddr) {
        self.nodes.insert(
            node_id,
            NodeInfo {
                node_id,
                client_addr,
                peer_addr,
            },
        );
    }

    /// Removes a node from the topology.
    pub fn remove_node(&mut self, node_id: i32) {
        self.nodes.remove(&node_id);
    }

    /// Total number of nodes in the cluster.
    #[inline]
    pub fn cluster_size(&self) -> usize {
        self.nodes.len()
    }

    /// Quorum size: majority $\lfloor N/2 \rfloor + 1$.
    #[inline]
    pub fn quorum_size(&self) -> usize {
        (self.nodes.len() / 2) + 1
    }

    /// Evaluates if `count` nodes constitutes a quorum majority.
    #[inline]
    pub fn is_quorum(&self, count: usize) -> bool {
        count >= self.quorum_size()
    }

    /// Returns list of peer node addresses (excluding self).
    pub fn peers(&self) -> Vec<(i32, SocketAddr)> {
        self.nodes
            .iter()
            .filter(|(&id, _)| id != self.local_node_id)
            .map(|(&id, info)| (id, info.peer_addr))
            .collect()
    }

    /// Returns `BrokerMetadata` for all known nodes in the cluster.
    pub fn to_broker_metadata(&self) -> Vec<BrokerMetadata> {
        self.nodes
            .values()
            .map(|info| {
                BrokerMetadata::new(
                    info.node_id,
                    info.client_addr.ip().to_string(),
                    info.client_addr.port() as i32,
                )
            })
            .collect()
    }

    /// Constructs `PartitionMetadata` for a partition.
    pub fn to_partition_metadata(&self, topic: &str, partition_id: u32) -> PartitionMetadata {
        if let Some(part) = self.partitions.get(&(topic.to_string(), partition_id)) {
            PartitionMetadata {
                error_code: ErrorCode::None,
                partition_index: partition_id as i32,
                leader_id: part.leader_id.unwrap_or(-1),
                leader_epoch: Some(part.leader_epoch as i32),
                replica_nodes: part.replicas.clone(),
                isr_nodes: part.isr.clone(),
                offline_replicas: Vec::new(),
            }
        } else {
            // Default single-partition configuration
            let replicas: Vec<i32> = self.nodes.keys().copied().collect();
            PartitionMetadata {
                error_code: ErrorCode::None,
                partition_index: partition_id as i32,
                leader_id: self.local_node_id,
                leader_epoch: Some(0),
                replica_nodes: replicas.clone(),
                isr_nodes: replicas,
                offline_replicas: Vec::new(),
            }
        }
    }
}
