//! Integration tests for distributed Raft consensus, leader election, and replication pipeline.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use aether_cluster::{ClusterEngine, PartitionProvider};
use aether_storage::{PartitionConfig, PartitionLog, Record};
use bytes::Bytes;
use parking_lot::RwLock;
use tempfile::{tempdir, TempDir};
use tokio::net::TcpListener;

type StoragePartitionMap = HashMap<(String, u32), Arc<RwLock<PartitionLog>>>;

struct TestPartitionStorage {
    dir: TempDir,
    config: PartitionConfig,
    partitions: RwLock<StoragePartitionMap>,
}

impl TestPartitionStorage {
    fn new() -> Self {
        Self {
            dir: tempdir().unwrap(),
            config: PartitionConfig {
                max_segment_bytes: 10 * 1024 * 1024,
                sync_on_append: false,
                max_index_bytes: 1024 * 1024,
                index_interval_bytes: 4096,
                retention_bytes: None,
                retention_ms: None,
            },
            partitions: RwLock::new(HashMap::new()),
        }
    }

    fn get_or_create(&self, topic: &str, partition_id: u32) -> Arc<RwLock<PartitionLog>> {
        let key = (topic.to_string(), partition_id);
        let mut parts = self.partitions.write();
        if let Some(part) = parts.get(&key) {
            return Arc::clone(part);
        }

        let part_dir = self.dir.path().join(format!("{}-{}", topic, partition_id));
        let log = PartitionLog::open(part_dir, topic, partition_id, self.config.clone()).unwrap();
        let arc = Arc::new(RwLock::new(log));
        parts.insert(key, Arc::clone(&arc));
        arc
    }
}

impl PartitionProvider for TestPartitionStorage {
    fn append_records(
        &self,
        topic: &str,
        partition_id: u32,
        records: &mut [Record],
    ) -> Result<u64, aether_cluster::ClusterError> {
        let part = self.get_or_create(topic, partition_id);
        let mut log = part.write();
        log.append_batch(records)?;
        Ok(log.log_end_offset())
    }

    fn get_leo(&self, topic: &str, partition_id: u32) -> Result<u64, aether_cluster::ClusterError> {
        let part = self.get_or_create(topic, partition_id);
        let leo = part.read().log_end_offset();
        Ok(leo)
    }

    fn read_records(
        &self,
        topic: &str,
        partition_id: u32,
        start_offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<Record>, aether_cluster::ClusterError> {
        let part = self.get_or_create(topic, partition_id);
        let records = part.read().read_records_from(start_offset, max_bytes)?;
        Ok(records)
    }
}

struct TestNode {
    engine: Arc<ClusterEngine>,
    storage: Arc<TestPartitionStorage>,
    client_addr: SocketAddr,
    peer_addr: SocketAddr,
}

async fn create_test_node(node_id: i32) -> TestNode {
    let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_addr = client_listener.local_addr().unwrap();

    let peer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = peer_listener.local_addr().unwrap();
    drop(peer_listener); // Let engine bind to it

    let storage = Arc::new(TestPartitionStorage::new());
    let engine = Arc::new(
        ClusterEngine::new(
            node_id,
            peer_addr,
            client_addr,
            Arc::clone(&storage) as Arc<dyn PartitionProvider>,
        )
        .with_timeouts(
            Duration::from_millis(150), // Fast election timeout
            Duration::from_millis(40),  // Heartbeat interval
            Duration::from_millis(100), // RPC timeout
        ),
    );

    engine.start().await.unwrap();

    TestNode {
        engine,
        storage,
        client_addr,
        peer_addr,
    }
}

#[tokio::test]
async fn test_three_node_cluster_election_replication_failover() {
    // 1. Spin up 3 nodes
    let node1 = create_test_node(1).await;
    let node2 = create_test_node(2).await;
    let node3 = create_test_node(3).await;

    // Cross-link peer topologies
    node1.engine.add_peer(2, node2.client_addr, node2.peer_addr);
    node1.engine.add_peer(3, node3.client_addr, node3.peer_addr);

    node2.engine.add_peer(1, node1.client_addr, node1.peer_addr);
    node2.engine.add_peer(3, node3.client_addr, node3.peer_addr);

    node3.engine.add_peer(1, node1.client_addr, node1.peer_addr);
    node3.engine.add_peer(2, node2.client_addr, node2.peer_addr);

    let nodes = vec![&node1, &node2, &node3];

    // 2. Wait for a leader to be elected
    let mut leader_idx = None;
    for iter in 0..50 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        for (i, node) in nodes.iter().enumerate() {
            if iter % 10 == 0 {
                println!(
                    "iter {} Node {} is_leader={} term={}",
                    iter,
                    node.engine.node_id(),
                    node.engine.is_leader(),
                    node.engine.current_term()
                );
            }
            if node.engine.is_leader() {
                leader_idx = Some(i);
                break;
            }
        }
        if leader_idx.is_some() {
            break;
        }
    }

    assert!(
        leader_idx.is_some(),
        "Leader election must elect a leader in quorum"
    );
    let initial_leader_idx = leader_idx.unwrap();
    let leader = nodes[initial_leader_idx];
    let leader_id = leader.engine.node_id();

    // 3. Produce records with acks = -1 (quorum commit) to the leader
    let mut batch1: Vec<Record> = (0..5)
        .map(|i| {
            Record::new(
                Some(Bytes::from(format!("k-{}", i))),
                Some(Bytes::from(format!("v-{}", i))),
            )
        })
        .collect();

    let new_leo = leader
        .engine
        .replicate_produce("orders", 0, &mut batch1, -1, Duration::from_secs(3))
        .await
        .expect("Quorum produce must succeed");

    assert_eq!(new_leo, 5);

    // Give followers a brief instant to acknowledge heartbeats
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Verify all nodes have the 5 records in their partition logs!
    for node in &nodes {
        let leo = node.storage.get_leo("orders", 0).unwrap();
        assert_eq!(
            leo,
            5,
            "Node {} must have replicated all 5 records",
            node.engine.node_id()
        );
    }

    // 4. Simulate Leader Kill / Crash
    leader.engine.shutdown();
    let surviving: Vec<&&TestNode> = nodes
        .iter()
        .filter(|n| n.engine.node_id() != leader_id)
        .collect();
    assert_eq!(surviving.len(), 2);

    // 5. Verify Failover: Surviving nodes must elect a new Leader
    let mut new_leader_node: Option<&TestNode> = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        for node in &surviving {
            if node.engine.is_leader() {
                new_leader_node = Some(node);
                break;
            }
        }
        if new_leader_node.is_some() {
            break;
        }
    }

    assert!(
        new_leader_node.is_some(),
        "Surviving quorum must elect a new leader"
    );
    let new_leader = new_leader_node.unwrap();
    assert_ne!(new_leader.engine.node_id(), leader_id);

    // 6. Produce new records to the new leader with acks = -1
    let mut batch2: Vec<Record> = (5..10)
        .map(|i| {
            Record::new(
                Some(Bytes::from(format!("k-{}", i))),
                Some(Bytes::from(format!("v-{}", i))),
            )
        })
        .collect();

    let leo2 = new_leader
        .engine
        .replicate_produce("orders", 0, &mut batch2, -1, Duration::from_secs(3))
        .await
        .expect("Produce on new leader must succeed");

    assert_eq!(leo2, 10);

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Verify surviving nodes have all 10 records
    for node in &surviving {
        let leo = node.storage.get_leo("orders", 0).unwrap();
        assert_eq!(
            leo,
            10,
            "Surviving node {} must have 10 records",
            node.engine.node_id()
        );

        // Verify content integrity of all 10 records
        let records = node.storage.read_records("orders", 0, 0, 10000).unwrap();
        assert_eq!(records.len(), 10);
        for (i, rec) in records.iter().enumerate() {
            assert_eq!(rec.offset, i as u64);
            assert_eq!(rec.key.as_ref().unwrap(), &Bytes::from(format!("k-{}", i)));
            assert_eq!(
                rec.value.as_ref().unwrap(),
                &Bytes::from(format!("v-{}", i))
            );
        }
    }

    // Clean shutdown of remaining nodes
    node2.engine.shutdown();
    node3.engine.shutdown();
}
