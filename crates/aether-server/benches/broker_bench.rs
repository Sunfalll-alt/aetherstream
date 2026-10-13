//! Criterion macro-benchmarks for AetherStream broker end-to-end performance.
//!
//! Measures end-to-end Produce throughput (MB/s) and Fetch latency distribution (p50, p95, p99).

use std::net::SocketAddr;

use aether_protocol::{
    ApiKey, FetchRequest, KafkaCodec, PartitionProduceData, ProduceRequest, RequestHeader,
    TopicProduceData,
};
use aether_server::{AetherServer, ServerConfig};
use aether_storage::{PartitionConfig, Record};
use bytes::{Bytes, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures_util::{SinkExt, StreamExt};
use tempfile::{tempdir, TempDir};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio_util::codec::Framed;

async fn start_benchmark_broker() -> (SocketAddr, TempDir, oneshot::Sender<()>) {
    let dir = tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_config = ServerConfig {
        bind_addr: local_addr.to_string(),
        data_dir: dir.path().to_path_buf(),
        node_id: 1,
        advertised_host: local_addr.ip().to_string(),
        advertised_port: local_addr.port() as i32,
        partition_config: PartitionConfig {
            max_segment_bytes: 1024 * 1024 * 1024, // 1 GiB
            sync_on_append: false,
            max_index_bytes: 10 * 1024 * 1024,
            index_interval_bytes: 4096,
            retention_bytes: None,
            retention_ms: None,
        },
        cluster_bind_addr: None,
        cluster_peers: Vec::new(),
    };

    let server = AetherServer::new(server_config).unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
        let _ = server
            .run_with_listener(listener, async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    (local_addr, dir, shutdown_tx)
}

fn bench_produce_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("broker_produce_throughput");

    let batch_records_count = 50;
    let payload_size = 1024; // 1 KB per record

    let mut records_buf = BytesMut::new();
    for i in 0..batch_records_count {
        let key = Bytes::from(format!("bench-key-{}", i));
        let val = Bytes::from(vec![b'A'; payload_size]);
        let rec = Record::new(Some(key), Some(val));
        rec.encode_into(&mut records_buf);
    }
    let batch_payload = records_buf.freeze();
    let total_batch_bytes = batch_payload.len();

    group.throughput(Throughput::Bytes(total_batch_bytes as u64));

    group.bench_function(
        BenchmarkId::new(
            "produce_batch_50x1KB",
            format!("{}KB", total_batch_bytes / 1024),
        ),
        |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let batch_payload = batch_payload.clone();
                async move {
                    let (broker_addr, _dir, shutdown_tx) = start_benchmark_broker().await;
                    let stream = TcpStream::connect(broker_addr).await.unwrap();
                    let mut client = Framed::new(stream, KafkaCodec::default());

                    let start = std::time::Instant::now();
                    for corr_id in 1..=(iters as i32) {
                        let produce_req = ProduceRequest {
                            transactional_id: None,
                            acks: 1,
                            timeout_ms: 5000,
                            topic_data: vec![TopicProduceData {
                                name: "bench-produce-topic".to_string(),
                                partition_data: vec![PartitionProduceData {
                                    partition_index: 0,
                                    records: Some(batch_payload.clone()),
                                }],
                            }],
                        };

                        let header = RequestHeader {
                            api_key: ApiKey::Produce,
                            api_version: 7,
                            correlation_id: corr_id,
                            client_id: Some("bench-producer".to_string()),
                        };

                        let mut buf = BytesMut::new();
                        header.encode(&mut buf);
                        produce_req.encode(7, &mut buf);

                        client.send(buf.freeze()).await.unwrap();
                        let resp = client.next().await.unwrap().unwrap();
                        black_box(resp);
                    }
                    let elapsed = start.elapsed();

                    let _ = shutdown_tx.send(());
                    elapsed
                }
            });
        },
    );

    group.finish();
}

fn bench_fetch_latency_profiles(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("broker_fetch_latency_profiles");

    let window_sizes = [4096i32, 65536, 1048576]; // 4KB, 64KB, 1MB

    for &max_bytes in &window_sizes {
        group.throughput(Throughput::Bytes(max_bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("fetch_zero_copy", format!("{}B", max_bytes)),
            &max_bytes,
            |b, &max_fetch_bytes| {
                b.to_async(&rt).iter_custom(|iters| async move {
                    let (broker_addr, _dir, shutdown_tx) = start_benchmark_broker().await;
                    let stream = TcpStream::connect(broker_addr).await.unwrap();
                    let mut client = Framed::new(stream, KafkaCodec::default());

                    // Seed topic with 500 records of 512 bytes
                    let mut records_buf = BytesMut::new();
                    for i in 0..500 {
                        let rec = Record::new(
                            Some(Bytes::from(format!("seed-k-{}", i))),
                            Some(Bytes::from(vec![b'Z'; 512])),
                        );
                        rec.encode_into(&mut records_buf);
                    }
                    let seed_payload = records_buf.freeze();

                    let produce_req = ProduceRequest {
                        transactional_id: None,
                        acks: 1,
                        timeout_ms: 5000,
                        topic_data: vec![TopicProduceData {
                            name: "bench-fetch-topic".to_string(),
                            partition_data: vec![PartitionProduceData {
                                partition_index: 0,
                                records: Some(seed_payload),
                            }],
                        }],
                    };
                    let header = RequestHeader {
                        api_key: ApiKey::Produce,
                        api_version: 7,
                        correlation_id: 1,
                        client_id: Some("seeder".to_string()),
                    };
                    let mut p_buf = BytesMut::new();
                    header.encode(&mut p_buf);
                    produce_req.encode(7, &mut p_buf);
                    client.send(p_buf.freeze()).await.unwrap();
                    let _ = client.next().await.unwrap().unwrap();

                    let start = std::time::Instant::now();

                    for corr_id in 2..=(1 + iters as i32) {
                        let fetch_req =
                            FetchRequest::new("bench-fetch-topic", 0, 0, max_fetch_bytes);

                        let f_header = RequestHeader {
                            api_key: ApiKey::Fetch,
                            api_version: 11,
                            correlation_id: corr_id,
                            client_id: Some("bench-consumer".to_string()),
                        };

                        let mut f_buf = BytesMut::new();
                        f_header.encode(&mut f_buf);
                        fetch_req.encode(11, &mut f_buf);

                        client.send(f_buf.freeze()).await.unwrap();
                        let resp = client.next().await.unwrap().unwrap();
                        black_box(resp);
                    }

                    let elapsed = start.elapsed();
                    let _ = shutdown_tx.send(());
                    elapsed
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_produce_throughput,
    bench_fetch_latency_profiles
);
criterion_main!(benches);
