//! Criterion micro-benchmarks for AetherStream storage primitives.
//!
//! Evaluates record serialization/deserialization, SIMD CRC32 throughput,
//! sparse index binary search lookups, and vectorized batch write efficiency.

use aether_storage::{Index, IndexConfig, Record, Segment, SegmentConfig};
use bytes::{Bytes, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tempfile::tempdir;

fn bench_record_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_serialization");

    let payload_sizes = [64, 1024, 65536];
    for &size in &payload_sizes {
        let key = Bytes::from(vec![b'k'; 16]);
        let val = Bytes::from(vec![b'v'; size]);
        let record = Record::new(Some(key), Some(val)).with_offset(100);
        let encoded_size = record.encoded_size();

        group.throughput(Throughput::Bytes(encoded_size as u64));
        group.bench_with_input(
            BenchmarkId::new("encode_into", format!("{}B", size)),
            &record,
            |b, rec| {
                let mut buf = BytesMut::with_capacity(encoded_size);
                b.iter(|| {
                    buf.clear();
                    rec.encode_into(&mut buf);
                    black_box(&buf);
                });
            },
        );
    }
    group.finish();
}

fn bench_record_deserialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_deserialization");

    let payload_sizes = [64, 1024, 65536];
    for &size in &payload_sizes {
        let key = Bytes::from(vec![b'k'; 16]);
        let val = Bytes::from(vec![b'v'; size]);
        let record = Record::new(Some(key), Some(val)).with_offset(100);
        let encoded_bytes = record.encode();

        group.throughput(Throughput::Bytes(encoded_bytes.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("decode_from_slice", format!("{}B", size)),
            &encoded_bytes,
            |b, bytes| {
                b.iter(|| {
                    let (rec, consumed) = Record::decode_from_slice(bytes).unwrap();
                    black_box((rec, consumed));
                });
            },
        );
    }
    group.finish();
}

fn bench_simd_crc32(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_crc32_throughput");

    let buffer_sizes = [64, 1024, 65536, 1048576];
    for &size in &buffer_sizes {
        let data = vec![0xABu8; size];

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::new("crc32fast", format!("{}B", size)),
            &data,
            |b, buf| {
                b.iter(|| {
                    let mut hasher = crc32fast::Hasher::new();
                    hasher.update(buf);
                    black_box(hasher.finalize());
                });
            },
        );
    }
    group.finish();
}

fn bench_sparse_index_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("sparse_index_binary_search");

    let dir = tempdir().unwrap();
    let config = IndexConfig {
        max_index_bytes: 1024 * 1024,
        index_interval_bytes: 4096,
    };
    let index_path = dir.path().join("0.index");
    let mut index = Index::create(&index_path, 0, config).unwrap();

    // Populate index with 10,000 entries
    for i in 0..10_000u32 {
        index.append(i * 10, i * 4096).unwrap();
    }

    let target_offsets = [0u32, 50_000, 99_990];
    for &target in &target_offsets {
        group.bench_with_input(
            BenchmarkId::new("lookup_offset", target),
            &target,
            |b, &t| {
                b.iter(|| {
                    let entry = index.lookup(black_box(t));
                    black_box(entry);
                });
            },
        );
    }
    group.finish();
}

fn bench_vectorized_vs_single_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("disk_write_vectorization");

    let batch_size = 50;
    let rec_val = Bytes::from(vec![b'x'; 256]);

    group.bench_function("vectorized_append_batch_50", |b| {
        b.iter_with_setup(
            || {
                let dir = tempdir().unwrap();
                let config = SegmentConfig {
                    max_segment_bytes: 10 * 1024 * 1024,
                    sync_on_append: false,
                    max_index_bytes: 1024 * 1024,
                    index_interval_bytes: 4096,
                };
                let segment = Segment::create(dir.path(), 0, config).unwrap();
                let batch: Vec<Record> = (0..batch_size)
                    .map(|_| Record::new(None, Some(rec_val.clone())))
                    .collect();
                (dir, segment, batch)
            },
            |(_dir, mut segment, mut batch)| {
                let results = segment.append_batch(&mut batch).unwrap();
                black_box(results);
            },
        );
    });

    group.bench_function("sequential_single_append_50", |b| {
        b.iter_with_setup(
            || {
                let dir = tempdir().unwrap();
                let config = SegmentConfig {
                    max_segment_bytes: 10 * 1024 * 1024,
                    sync_on_append: false,
                    max_index_bytes: 1024 * 1024,
                    index_interval_bytes: 4096,
                };
                let segment = Segment::create(dir.path(), 0, config).unwrap();
                let batch: Vec<Record> = (0..batch_size)
                    .map(|_| Record::new(None, Some(rec_val.clone())))
                    .collect();
                (dir, segment, batch)
            },
            |(_dir, mut segment, mut batch)| {
                for rec in &mut batch {
                    let res = segment.append(rec).unwrap();
                    black_box(res);
                }
            },
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_record_serialization,
    bench_record_deserialization,
    bench_simd_crc32,
    bench_sparse_index_lookup,
    bench_vectorized_vs_single_append
);
criterion_main!(benches);
