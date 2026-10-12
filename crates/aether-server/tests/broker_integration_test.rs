//! End-to-end live broker integration tests.
//!
//! Spins up AetherServer on an ephemeral port, establishes client TCP streams,
//! and verifies ApiVersions, Metadata, Produce, and Fetch end-to-end.

use aether_protocol::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, FetchRequest, FetchResponse,
    KafkaCodec, MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse,
    RequestHeader,
};
use aether_server::{AetherServer, ServerConfig};
use aether_storage::{PartitionConfig, Record};
use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tempfile::tempdir;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_util::codec::Framed;

#[tokio::test]
async fn test_broker_full_produce_fetch_lifecycle() {
    let dir = tempdir().unwrap();

    // 1. Bind TCP listener on ephemeral port (127.0.0.1:0)
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_config = ServerConfig {
        bind_addr: local_addr.to_string(),
        data_dir: dir.path().to_path_buf(),
        node_id: 1,
        advertised_host: local_addr.ip().to_string(),
        advertised_port: local_addr.port() as i32,
        partition_config: PartitionConfig {
            max_segment_bytes: 1024 * 1024,
            sync_on_append: false,
            max_index_bytes: 1024 * 1024,
            index_interval_bytes: 100,
            retention_bytes: None,
            retention_ms: None,
        },
    };

    let server = AetherServer::new(server_config).unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server_handle = tokio::spawn(async move {
        server
            .run_with_listener(listener, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    // 2. Client connects over TCP
    let stream = TcpStream::connect(local_addr).await.unwrap();
    let mut client = Framed::new(stream, KafkaCodec::default());

    // -------------------------------------------------------------------------
    // Step 1: ApiVersions Handshake
    // -------------------------------------------------------------------------
    let req_header = RequestHeader {
        api_key: ApiKey::ApiVersions,
        api_version: 3,
        correlation_id: 101,
        client_id: Some("integration-test-client".to_string()),
    };

    let mut req_buf = BytesMut::new();
    req_header.encode(&mut req_buf);
    ApiVersionsRequest::default().encode(3, &mut req_buf);

    client.send(req_buf.freeze()).await.unwrap();

    let resp_frame = client.next().await.unwrap().unwrap();
    let mut resp_cursor = resp_frame;
    // Skip correlation_id (4 bytes) + tagged fields (1 byte for v1 response header in v3)
    assert!(resp_cursor.len() >= 5);
    let _ = resp_cursor.split_to(5);

    let api_versions_resp = ApiVersionsResponse::decode(3, &mut resp_cursor).unwrap();
    assert_eq!(api_versions_resp.error_code, aether_protocol::ErrorCode::None);
    assert!(api_versions_resp.api_keys.len() >= 4);

    // -------------------------------------------------------------------------
    // Step 2: Metadata Discovery (Dynamic Topic Creation)
    // -------------------------------------------------------------------------
    let req_header = RequestHeader {
        api_key: ApiKey::Metadata,
        api_version: 5,
        correlation_id: 102,
        client_id: Some("integration-test-client".to_string()),
    };

    let mut req_buf = BytesMut::new();
    req_header.encode(&mut req_buf);
    MetadataRequest::for_topics(vec!["device-telemetry".to_string()]).encode(5, &mut req_buf);

    client.send(req_buf.freeze()).await.unwrap();

    let resp_frame = client.next().await.unwrap().unwrap();
    let mut resp_cursor = resp_frame;
    // Skip 4-byte correlation ID
    let _ = resp_cursor.split_to(4);

    let metadata_resp = MetadataResponse::decode(5, &mut resp_cursor).unwrap();
    assert_eq!(metadata_resp.brokers.len(), 1);
    assert_eq!(metadata_resp.brokers[0].node_id, 1);
    assert_eq!(metadata_resp.topics.len(), 1);
    assert_eq!(metadata_resp.topics[0].name, "device-telemetry");
    assert_eq!(metadata_resp.topics[0].partitions.len(), 1);
    assert_eq!(metadata_resp.topics[0].partitions[0].partition_index, 0);
    assert_eq!(metadata_resp.topics[0].partitions[0].leader_id, 1);

    // -------------------------------------------------------------------------
    // Step 3: Produce Records
    // -------------------------------------------------------------------------
    let rec1 = Record::new(
        Some(Bytes::from_static(b"sensor-1")),
        Some(Bytes::from_static(b"payload-reading-100")),
    );
    let rec2 = Record::new(
        Some(Bytes::from_static(b"sensor-2")),
        Some(Bytes::from_static(b"payload-reading-200")),
    );

    let mut batch_buf = BytesMut::new();
    rec1.encode_into(&mut batch_buf);
    rec2.encode_into(&mut batch_buf);

    let req_header = RequestHeader {
        api_key: ApiKey::Produce,
        api_version: 5,
        correlation_id: 103,
        client_id: Some("integration-test-client".to_string()),
    };

    let produce_req = ProduceRequest::new(
        "device-telemetry",
        0,
        batch_buf.freeze(),
        1, // acks = 1
    );

    let mut req_buf = BytesMut::new();
    req_header.encode(&mut req_buf);
    produce_req.encode(5, &mut req_buf);

    client.send(req_buf.freeze()).await.unwrap();

    let resp_frame = client.next().await.unwrap().unwrap();
    let mut resp_cursor = resp_frame;
    let _ = resp_cursor.split_to(4); // Skip correlation_id

    let produce_resp = ProduceResponse::decode(5, &mut resp_cursor).unwrap();
    assert_eq!(produce_resp.responses.len(), 1);
    assert_eq!(produce_resp.responses[0].name, "device-telemetry");
    assert_eq!(produce_resp.responses[0].partition_responses.len(), 1);
    assert_eq!(
        produce_resp.responses[0].partition_responses[0].base_offset,
        0
    );
    assert_eq!(
        produce_resp.responses[0].partition_responses[0].error_code,
        aether_protocol::ErrorCode::None
    );

    // -------------------------------------------------------------------------
    // Step 4: Fetch Records from Offset 0
    // -------------------------------------------------------------------------
    let req_header = RequestHeader {
        api_key: ApiKey::Fetch,
        api_version: 5,
        correlation_id: 104,
        client_id: Some("integration-test-client".to_string()),
    };

    let fetch_req = FetchRequest::new("device-telemetry", 0, 0, 65536);

    let mut req_buf = BytesMut::new();
    req_header.encode(&mut req_buf);
    fetch_req.encode(5, &mut req_buf);

    client.send(req_buf.freeze()).await.unwrap();

    let resp_frame = client.next().await.unwrap().unwrap();
    let mut resp_cursor = resp_frame;
    let _ = resp_cursor.split_to(4); // Skip correlation_id

    let fetch_resp = FetchResponse::decode(5, &mut resp_cursor).unwrap();
    assert_eq!(fetch_resp.responses.len(), 1);
    assert_eq!(fetch_resp.responses[0].name, "device-telemetry");
    assert_eq!(fetch_resp.responses[0].partitions.len(), 1);
    let part_fetch = &fetch_resp.responses[0].partitions[0];
    assert_eq!(part_fetch.error_code, aether_protocol::ErrorCode::None);
    assert_eq!(part_fetch.high_watermark, 2);

    // Verify fetched record contents
    let records_payload = part_fetch.records.as_ref().expect("Records payload expected");
    let mut slice: &[u8] = records_payload;

    let (decoded_rec1, consumed1) = Record::decode_from_slice(slice).unwrap();
    assert_eq!(decoded_rec1.offset, 0);
    assert_eq!(decoded_rec1.key, Some(Bytes::from_static(b"sensor-1")));
    assert_eq!(
        decoded_rec1.value,
        Some(Bytes::from_static(b"payload-reading-100"))
    );

    slice = &slice[consumed1..];
    let (decoded_rec2, consumed2) = Record::decode_from_slice(slice).unwrap();
    assert_eq!(decoded_rec2.offset, 1);
    assert_eq!(decoded_rec2.key, Some(Bytes::from_static(b"sensor-2")));
    assert_eq!(
        decoded_rec2.value,
        Some(Bytes::from_static(b"payload-reading-200"))
    );
    assert_eq!(slice.len(), consumed2);

    // -------------------------------------------------------------------------
    // Step 5: Fetch from Offset 1 (Subset)
    // -------------------------------------------------------------------------
    let req_header = RequestHeader {
        api_key: ApiKey::Fetch,
        api_version: 5,
        correlation_id: 105,
        client_id: Some("integration-test-client".to_string()),
    };

    let fetch_req2 = FetchRequest::new("device-telemetry", 0, 1, 65536);
    let mut req_buf = BytesMut::new();
    req_header.encode(&mut req_buf);
    fetch_req2.encode(5, &mut req_buf);

    client.send(req_buf.freeze()).await.unwrap();

    let resp_frame = client.next().await.unwrap().unwrap();
    let mut resp_cursor = resp_frame;
    let _ = resp_cursor.split_to(4);

    let fetch_resp2 = FetchResponse::decode(5, &mut resp_cursor).unwrap();
    let part_fetch2 = &fetch_resp2.responses[0].partitions[0];
    let records_payload2 = part_fetch2.records.as_ref().unwrap();

    let (decoded_rec_sub, _) = Record::decode_from_slice(records_payload2).unwrap();
    assert_eq!(decoded_rec_sub.offset, 1);
    assert_eq!(decoded_rec_sub.key, Some(Bytes::from_static(b"sensor-2")));

    // -------------------------------------------------------------------------
    // Step 6: Graceful Server Shutdown
    // -------------------------------------------------------------------------
    let _ = shutdown_tx.send(());
    let _ = server_handle.await;
}
