//! End-to-end integration test verifying distributed 3-node cluster operation,
//! dynamic Raft leader election, cluster metadata discovery, quorum-replicated
//! produce with acks = -1, follower alignment, and not-leader rejection.

use std::net::SocketAddr;
use std::time::Duration;

use aether_protocol::{
    ApiKey, ErrorCode, FetchRequest, FetchResponse, KafkaCodec, MetadataRequest, MetadataResponse,
    PartitionProduceData, ProduceRequest, ProduceResponse, RequestHeader, TopicProduceData,
};
use aether_server::{AetherServer, ServerConfig};
use aether_storage::{PartitionConfig, Record};
use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use tempfile::tempdir;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_util::codec::Framed;

#[tokio::test]
async fn test_three_node_broker_cluster_e2e() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    // 1. Bind ephemeral TCP listeners for clients & inter-broker Raft
    let client_l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let raft_l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let c_addr1 = client_l1.local_addr().unwrap();
    let r_addr1 = raft_l1.local_addr().unwrap();
    drop(raft_l1);

    let client_l2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let raft_l2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let c_addr2 = client_l2.local_addr().unwrap();
    let r_addr2 = raft_l2.local_addr().unwrap();
    drop(raft_l2);

    let client_l3 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let raft_l3 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let c_addr3 = client_l3.local_addr().unwrap();
    let r_addr3 = raft_l3.local_addr().unwrap();
    drop(raft_l3);

    let part_cfg = PartitionConfig {
        max_segment_bytes: 1024 * 1024,
        sync_on_append: false,
        max_index_bytes: 1024 * 1024,
        index_interval_bytes: 100,
        retention_bytes: None,
        retention_ms: None,
    };

    // Configure Node 1
    let cfg1 = ServerConfig {
        bind_addr: c_addr1.to_string(),
        data_dir: dir1.path().to_path_buf(),
        node_id: 1,
        advertised_host: c_addr1.ip().to_string(),
        advertised_port: c_addr1.port() as i32,
        partition_config: part_cfg.clone(),
        cluster_bind_addr: Some(r_addr1.to_string()),
        cluster_peers: vec![(2, c_addr2, r_addr2), (3, c_addr3, r_addr3)],
    };

    // Configure Node 2
    let cfg2 = ServerConfig {
        bind_addr: c_addr2.to_string(),
        data_dir: dir2.path().to_path_buf(),
        node_id: 2,
        advertised_host: c_addr2.ip().to_string(),
        advertised_port: c_addr2.port() as i32,
        partition_config: part_cfg.clone(),
        cluster_bind_addr: Some(r_addr2.to_string()),
        cluster_peers: vec![(1, c_addr1, r_addr1), (3, c_addr3, r_addr3)],
    };

    // Configure Node 3
    let cfg3 = ServerConfig {
        bind_addr: c_addr3.to_string(),
        data_dir: dir3.path().to_path_buf(),
        node_id: 3,
        advertised_host: c_addr3.ip().to_string(),
        advertised_port: c_addr3.port() as i32,
        partition_config: part_cfg.clone(),
        cluster_bind_addr: Some(r_addr3.to_string()),
        cluster_peers: vec![(1, c_addr1, r_addr1), (2, c_addr2, r_addr2)],
    };

    let server1 = AetherServer::new(cfg1).unwrap();
    let server2 = AetherServer::new(cfg2).unwrap();
    let server3 = AetherServer::new(cfg3).unwrap();

    let state1 = server1.state().clone();
    let state2 = server2.state().clone();
    let state3 = server3.state().clone();

    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let (tx3, rx3) = oneshot::channel();

    let h1 = tokio::spawn(async move {
        server1
            .run_with_listener(client_l1, async move {
                let _ = rx1.await;
            })
            .await
            .unwrap();
    });
    let h2 = tokio::spawn(async move {
        server2
            .run_with_listener(client_l2, async move {
                let _ = rx2.await;
            })
            .await
            .unwrap();
    });
    let h3 = tokio::spawn(async move {
        server3
            .run_with_listener(client_l3, async move {
                let _ = rx3.await;
            })
            .await
            .unwrap();
    });

    // 2. Wait for leader election across the cluster
    let mut leader_id = None;
    let mut leader_client_addr: Option<SocketAddr> = None;

    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if let Some(c) = state1.cluster() {
            if c.is_leader() {
                leader_id = Some(1);
                leader_client_addr = Some(c_addr1);
                break;
            }
        }
        if let Some(c) = state2.cluster() {
            if c.is_leader() {
                leader_id = Some(2);
                leader_client_addr = Some(c_addr2);
                break;
            }
        }
        if let Some(c) = state3.cluster() {
            if c.is_leader() {
                leader_id = Some(3);
                leader_client_addr = Some(c_addr3);
                break;
            }
        }
    }

    let leader_id = leader_id.expect("A leader must be elected in the cluster within timeout");
    let leader_client_addr = leader_client_addr.expect("Leader client address must be known");

    // -------------------------------------------------------------------------
    // Step 1: Query Metadata from Node 1 (verify dynamic cluster topology)
    // -------------------------------------------------------------------------
    let stream = TcpStream::connect(c_addr1).await.unwrap();
    let mut client1 = Framed::new(stream, KafkaCodec::default());

    let req_header = RequestHeader {
        api_key: ApiKey::Metadata,
        api_version: 5,
        correlation_id: 101,
        client_id: Some("cluster-test-client".to_string()),
    };
    let mut req_buf = BytesMut::new();
    req_header.encode(&mut req_buf);
    MetadataRequest::for_topics(vec!["cluster-events".to_string()]).encode(5, &mut req_buf);
    client1.send(req_buf.freeze()).await.unwrap();

    let resp_frame = client1.next().await.unwrap().unwrap();
    let mut resp_cursor = resp_frame;
    let _ = resp_cursor.split_to(4); // Skip correlation_id
    let metadata_resp = MetadataResponse::decode(5, &mut resp_cursor).unwrap();

    // Verify all 3 brokers are returned
    assert_eq!(metadata_resp.brokers.len(), 3);
    let broker_ids: Vec<i32> = metadata_resp.brokers.iter().map(|b| b.node_id).collect();
    assert!(broker_ids.contains(&1));
    assert!(broker_ids.contains(&2));
    assert!(broker_ids.contains(&3));

    // Verify controller and partition leader
    assert_eq!(metadata_resp.controller_id, leader_id);
    assert_eq!(metadata_resp.topics.len(), 1);
    assert_eq!(metadata_resp.topics[0].name, "cluster-events");
    let part_meta = &metadata_resp.topics[0].partitions[0];
    assert_eq!(part_meta.leader_id, leader_id);
    assert_eq!(part_meta.replica_nodes.len(), 3);
    assert_eq!(part_meta.isr_nodes.len(), 3);

    // -------------------------------------------------------------------------
    // Step 2: Produce to the Elected LEADER with acks = -1 (Quorum Persistence)
    // -------------------------------------------------------------------------
    let leader_stream = TcpStream::connect(leader_client_addr).await.unwrap();
    let mut leader_client = Framed::new(leader_stream, KafkaCodec::default());

    let rec1 = Record::new(Some("k1".into()), Some("payload-1".into()));
    let rec2 = Record::new(Some("k2".into()), Some("payload-2".into()));
    let rec3 = Record::new(Some("k3".into()), Some("payload-3".into()));
    let mut batch_buf = BytesMut::new();
    rec1.encode_into(&mut batch_buf);
    rec2.encode_into(&mut batch_buf);
    rec3.encode_into(&mut batch_buf);

    let req_header = RequestHeader {
        api_key: ApiKey::Produce,
        api_version: 7,
        correlation_id: 102,
        client_id: Some("cluster-test-client".to_string()),
    };
    let mut req_buf = BytesMut::new();
    req_header.encode(&mut req_buf);

    let produce_req = ProduceRequest {
        transactional_id: None,
        acks: -1, // Wait for full quorum commit
        timeout_ms: 3000,
        topic_data: vec![TopicProduceData {
            name: "cluster-events".to_string(),
            partition_data: vec![PartitionProduceData {
                partition_index: 0,
                records: Some(batch_buf.freeze()),
            }],
        }],
    };
    produce_req.encode(7, &mut req_buf);
    leader_client.send(req_buf.freeze()).await.unwrap();

    let resp_frame = leader_client.next().await.unwrap().unwrap();
    let mut resp_cursor = resp_frame;
    let _ = resp_cursor.split_to(4); // Skip correlation_id
    let produce_resp = ProduceResponse::decode(7, &mut resp_cursor).unwrap();

    assert_eq!(produce_resp.responses.len(), 1);
    let p_resp = &produce_resp.responses[0].partition_responses[0];
    assert_eq!(p_resp.error_code, ErrorCode::None);
    assert_eq!(p_resp.base_offset, 0);

    // -------------------------------------------------------------------------
    // Step 3: Fetch Records from Leader
    // -------------------------------------------------------------------------
    let req_header = RequestHeader {
        api_key: ApiKey::Fetch,
        api_version: 5,
        correlation_id: 103,
        client_id: Some("cluster-test-client".to_string()),
    };
    let mut req_buf = BytesMut::new();
    req_header.encode(&mut req_buf);

    let fetch_req = FetchRequest::new("cluster-events", 0, 0, 65536);
    fetch_req.encode(5, &mut req_buf);
    leader_client.send(req_buf.freeze()).await.unwrap();

    let resp_frame = leader_client.next().await.unwrap().unwrap();
    let mut resp_cursor = resp_frame;
    let _ = resp_cursor.split_to(4); // Skip correlation_id
    let fetch_resp = FetchResponse::decode(5, &mut resp_cursor).unwrap();

    assert_eq!(fetch_resp.responses.len(), 1);
    let f_part = &fetch_resp.responses[0].partitions[0];
    assert_eq!(f_part.error_code, ErrorCode::None);
    assert!(f_part.high_watermark >= 3);
    assert!(f_part.records.is_some());

    // -------------------------------------------------------------------------
    // Step 4: Verify Follower Rejects Direct Client Produce
    // -------------------------------------------------------------------------
    let (follower_addr, _) = if leader_id == 1 {
        (c_addr2, 2)
    } else {
        (c_addr1, 1)
    };

    let f_stream = TcpStream::connect(follower_addr).await.unwrap();
    let mut follower_client = Framed::new(f_stream, KafkaCodec::default());

    let req_header = RequestHeader {
        api_key: ApiKey::Produce,
        api_version: 7,
        correlation_id: 104,
        client_id: Some("cluster-test-client".to_string()),
    };
    let mut req_buf = BytesMut::new();
    req_header.encode(&mut req_buf);

    let produce_req_follower = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 1000,
        topic_data: vec![TopicProduceData {
            name: "cluster-events".to_string(),
            partition_data: vec![PartitionProduceData {
                partition_index: 0,
                records: Some("direct-follower-write".into()),
            }],
        }],
    };
    produce_req_follower.encode(7, &mut req_buf);
    follower_client.send(req_buf.freeze()).await.unwrap();

    let resp_frame = follower_client.next().await.unwrap().unwrap();
    let mut resp_cursor = resp_frame;
    let _ = resp_cursor.split_to(4);
    let follower_produce_resp = ProduceResponse::decode(7, &mut resp_cursor).unwrap();
    assert_eq!(
        follower_produce_resp.responses[0].partition_responses[0].error_code,
        ErrorCode::NotLeaderOrFollower
    );

    // -------------------------------------------------------------------------
    // Step 5: Simulate Leader Crash & Failover to New Leader
    // -------------------------------------------------------------------------
    drop(leader_client);
    drop(follower_client);
    drop(client1);

    // Stop the elected leader
    let mut txs = [Some(tx1), Some(tx2), Some(tx3)];
    let mut handles = [Some(h1), Some(h2), Some(h3)];

    let leader_idx = (leader_id - 1) as usize;
    let _ = txs[leader_idx].take().unwrap().send(());
    let _ = handles[leader_idx].take().unwrap().await;

    // Wait for a new leader among remaining 2 nodes
    let mut new_leader_id = None;
    let mut new_leader_addr = None;

    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if leader_id != 1 {
            if let Some(c) = state1.cluster() {
                if c.is_leader() {
                    new_leader_id = Some(1);
                    new_leader_addr = Some(c_addr1);
                    break;
                }
            }
        }
        if leader_id != 2 {
            if let Some(c) = state2.cluster() {
                if c.is_leader() {
                    new_leader_id = Some(2);
                    new_leader_addr = Some(c_addr2);
                    break;
                }
            }
        }
        if leader_id != 3 {
            if let Some(c) = state3.cluster() {
                if c.is_leader() {
                    new_leader_id = Some(3);
                    new_leader_addr = Some(c_addr3);
                    break;
                }
            }
        }
    }

    let new_leader_id = new_leader_id.expect("A new leader must be elected after crash");
    let new_leader_addr = new_leader_addr.expect("New leader address must be known");
    assert_ne!(new_leader_id, leader_id);

    // Connect to new leader
    let new_leader_stream = TcpStream::connect(new_leader_addr).await.unwrap();
    let mut new_leader_client = Framed::new(new_leader_stream, KafkaCodec::default());

    // Produce more records to the new leader with acks = -1
    let rec4 = Record::new(Some("k4".into()), Some("payload-4".into()));
    let mut batch_buf2 = BytesMut::new();
    rec4.encode_into(&mut batch_buf2);

    let req_header = RequestHeader {
        api_key: ApiKey::Produce,
        api_version: 7,
        correlation_id: 105,
        client_id: Some("cluster-test-client".to_string()),
    };
    let mut req_buf = BytesMut::new();
    req_header.encode(&mut req_buf);

    let produce_req2 = ProduceRequest {
        transactional_id: None,
        acks: -1,
        timeout_ms: 3000,
        topic_data: vec![TopicProduceData {
            name: "cluster-events".to_string(),
            partition_data: vec![PartitionProduceData {
                partition_index: 0,
                records: Some(batch_buf2.freeze()),
            }],
        }],
    };
    produce_req2.encode(7, &mut req_buf);
    new_leader_client.send(req_buf.freeze()).await.unwrap();

    let resp_frame = new_leader_client.next().await.unwrap().unwrap();
    let mut resp_cursor = resp_frame;
    let _ = resp_cursor.split_to(4);
    let p_resp2 = ProduceResponse::decode(7, &mut resp_cursor).unwrap();
    assert_eq!(
        p_resp2.responses[0].partition_responses[0].error_code,
        ErrorCode::None
    );
    assert_eq!(p_resp2.responses[0].partition_responses[0].base_offset, 3);

    // Fetch all records from new leader (from offset 0)
    let req_header = RequestHeader {
        api_key: ApiKey::Fetch,
        api_version: 5,
        correlation_id: 106,
        client_id: Some("cluster-test-client".to_string()),
    };
    let mut req_buf = BytesMut::new();
    req_header.encode(&mut req_buf);

    let fetch_req_all = FetchRequest::new("cluster-events", 0, 0, 65536);
    fetch_req_all.encode(5, &mut req_buf);
    new_leader_client.send(req_buf.freeze()).await.unwrap();

    let resp_frame = new_leader_client.next().await.unwrap().unwrap();
    let mut resp_cursor = resp_frame;
    let _ = resp_cursor.split_to(4);
    let fetch_resp_all = FetchResponse::decode(5, &mut resp_cursor).unwrap();
    let f_part_all = &fetch_resp_all.responses[0].partitions[0];
    assert_eq!(f_part_all.error_code, ErrorCode::None);
    assert!(f_part_all.high_watermark >= 4);

    // -------------------------------------------------------------------------
    // Step 6: Graceful Shutdown of Remaining Nodes
    // -------------------------------------------------------------------------
    for tx in txs.into_iter().flatten() {
        let _ = tx.send(());
    }
    for h in handles.into_iter().flatten() {
        let _ = h.await;
    }
}
