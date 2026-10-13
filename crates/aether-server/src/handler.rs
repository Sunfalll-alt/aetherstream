//! Request router and API dispatch handlers.

use std::sync::Arc;

use aether_protocol::{
    ApiKey, ApiVersionsResponse, BrokerMetadata, ErrorCode, FetchPartitionResponse,
    FetchRequest, FetchResponse, FetchTopicResponse, InboundRequest, MetadataRequest,
    MetadataResponse, PartitionMetadata, PartitionProduceResponse, ProduceRequest,
    ProduceResponse, ResponseHeader, TopicMetadata, TopicProduceResponse,
};
use aether_storage::Record;
use bytes::{Bytes, BytesMut};
use tracing::{debug, warn};

use crate::error::ServerError;
use crate::state::BrokerState;

/// Dispatches an incoming parsed request to the appropriate API handler.
///
/// Returns the framed response bytes (with `ResponseHeader`), or empty `Bytes` if no response
/// is expected (such as `Produce` with `acks = 0`).
pub async fn handle_request(
    state: Arc<BrokerState>,
    mut req: InboundRequest,
) -> Result<Bytes, ServerError> {
    match req.header.api_key {
        ApiKey::ApiVersions => handle_api_versions(&req),
        ApiKey::Metadata => handle_metadata(&state, &mut req),
        ApiKey::Produce => handle_produce(&state, &mut req),
        ApiKey::Fetch => handle_fetch(&state, &mut req),
        unsupported => {
            warn!("Received unsupported API request: {:?}", unsupported);
            Err(ServerError::Protocol(
                aether_protocol::ProtocolError::UnsupportedApiKey(unsupported.as_i16()),
            ))
        }
    }
}

/// Handles `ApiVersions` requests (v0 - v3).
fn handle_api_versions(req: &InboundRequest) -> Result<Bytes, ServerError> {
    debug!(
        "Handling ApiVersions v{} (correlation_id: {})",
        req.header.api_version, req.header.correlation_id
    );

    let mut buf = BytesMut::new();
    let resp_header = ResponseHeader::new(req.header.correlation_id);

    if req.header.api_version <= 3 {
        let resp = ApiVersionsResponse::default_supported();
        if req.header.api_version >= 3 {
            resp_header.encode_v1(&mut buf);
        } else {
            resp_header.encode(&mut buf);
        }
        resp.encode(req.header.api_version, &mut buf);
    } else {
        // Fall back to v0 error response so client downgrades
        let resp = ApiVersionsResponse::unsupported_version();
        resp_header.encode(&mut buf);
        resp.encode(0, &mut buf);
    }

    Ok(buf.freeze())
}

/// Handles `Metadata` requests (v0 - v7).
fn handle_metadata(
    state: &BrokerState,
    req: &mut InboundRequest,
) -> Result<Bytes, ServerError> {
    let metadata_req = MetadataRequest::decode(req.header.api_version, &mut req.body)?;

    debug!(
        "Handling Metadata v{} for topics: {:?}",
        req.header.api_version, metadata_req.topics
    );

    let mut topic_metadata_list = Vec::new();

    match metadata_req.topics {
        None => {
            // Requesting ALL topics in the cluster
            for (topic_name, partition_indices) in state.list_topics() {
                let mut parts = Vec::new();
                for part_id in partition_indices {
                    parts.push(PartitionMetadata::leader(part_id as i32, state.node_id));
                }
                topic_metadata_list.push(TopicMetadata::success(topic_name, parts));
            }
        }
        Some(topics) if topics.is_empty() => {
            // Empty list in v0 also means ALL topics
            for (topic_name, partition_indices) in state.list_topics() {
                let mut parts = Vec::new();
                for part_id in partition_indices {
                    parts.push(PartitionMetadata::leader(part_id as i32, state.node_id));
                }
                topic_metadata_list.push(TopicMetadata::success(topic_name, parts));
            }
        }
        Some(topics) => {
            for topic_name in topics {
                if state.topic_exists(&topic_name) {
                    let mut parts = Vec::new();
                    // Load all known partitions for this topic
                    for (t, p) in state.list_topics() {
                        if t == topic_name {
                            for part_id in p {
                                parts.push(PartitionMetadata::leader(part_id as i32, state.node_id));
                            }
                        }
                    }
                    topic_metadata_list.push(TopicMetadata::success(topic_name, parts));
                } else if metadata_req.allow_auto_topic_creation {
                    // Dynamically create partition 0 for the requested topic
                    state.get_or_create_partition(&topic_name, 0)?;
                    let parts = vec![PartitionMetadata::leader(0, state.node_id)];
                    topic_metadata_list.push(TopicMetadata::success(topic_name, parts));
                } else {
                    topic_metadata_list.push(TopicMetadata {
                        error_code: ErrorCode::UnknownTopicOrPartition,
                        name: topic_name,
                        is_internal: false,
                        partitions: Vec::new(),
                    });
                }
            }
        }
    }

    let broker = BrokerMetadata::new(state.node_id, &state.host, state.port);
    let resp = MetadataResponse {
        throttle_time_ms: 0,
        brokers: vec![broker],
        cluster_id: Some("aether-cluster-local".to_string()),
        controller_id: state.node_id,
        topics: topic_metadata_list,
    };

    let mut buf = BytesMut::new();
    ResponseHeader::new(req.header.correlation_id).encode(&mut buf);
    resp.encode(req.header.api_version, &mut buf);

    Ok(buf.freeze())
}

/// Handles `Produce` requests (v3 - v8).
fn handle_produce(
    state: &BrokerState,
    req: &mut InboundRequest,
) -> Result<Bytes, ServerError> {
    let produce_req = ProduceRequest::decode(req.header.api_version, &mut req.body)?;

    debug!(
        "Handling Produce v{} (acks: {}, topics: {})",
        req.header.api_version,
        produce_req.acks,
        produce_req.topic_data.len()
    );

    let mut topic_responses = Vec::with_capacity(produce_req.topic_data.len());

    for topic_data in produce_req.topic_data {
        let mut partition_responses = Vec::with_capacity(topic_data.partition_data.len());

        for part_data in topic_data.partition_data {
            let part_id = part_data.partition_index;

            let part_res = match state.get_or_create_partition(&topic_data.name, part_id as u32) {
                Ok(part_arc) => {
                    if let Some(records_payload) = part_data.records {
                        let mut records = extract_records(records_payload);
                        let mut part_log = part_arc.write();

                        match part_log.append_batch(&mut records) {
                            Ok(results) => {
                                let base_offset = results
                                    .first()
                                    .map(|r| r.offset)
                                    .unwrap_or_else(|| part_log.log_end_offset());
                                let start_offset = part_log.log_start_offset();
                                PartitionProduceResponse::success(part_id, base_offset, start_offset)
                            }
                            Err(err) => {
                                warn!("Append error on {}-{}: {}", topic_data.name, part_id, err);
                                PartitionProduceResponse::error(part_id, ErrorCode::CorruptMessage)
                            }
                        }
                    } else {
                        // Empty records payload
                        let part_log = part_arc.read();
                        let next_offset = part_log.log_end_offset();
                        let start_offset = part_log.log_start_offset();
                        PartitionProduceResponse::success(part_id, next_offset, start_offset)
                    }
                }
                Err(e) => {
                    warn!("Failed opening partition {}-{}: {}", topic_data.name, part_id, e);
                    PartitionProduceResponse::error(part_id, ErrorCode::LeaderNotAvailable)
                }
            };

            partition_responses.push(part_res);
        }

        topic_responses.push(TopicProduceResponse {
            name: topic_data.name,
            partition_responses,
        });
    }

    // In Kafka, if acks == 0, the client expects NO response packet
    if produce_req.acks == 0 {
        return Ok(Bytes::new());
    }

    let resp = ProduceResponse {
        responses: topic_responses,
        throttle_time_ms: 0,
    };

    let mut buf = BytesMut::new();
    ResponseHeader::new(req.header.correlation_id).encode(&mut buf);
    resp.encode(req.header.api_version, &mut buf);

    Ok(buf.freeze())
}

/// Handles `Fetch` requests (v4 - v11).
fn handle_fetch(
    state: &BrokerState,
    req: &mut InboundRequest,
) -> Result<Bytes, ServerError> {
    let fetch_req = FetchRequest::decode(req.header.api_version, &mut req.body)?;

    debug!(
        "Handling Fetch v{} (topics: {})",
        req.header.api_version,
        fetch_req.topics.len()
    );

    let mut topic_responses = Vec::with_capacity(fetch_req.topics.len());

    for topic_target in fetch_req.topics {
        let mut partition_responses = Vec::with_capacity(topic_target.partitions.len());

        for part_target in topic_target.partitions {
            let part_id = part_target.partition_index;

            let part_res = match state.get_partition(&topic_target.name, part_id as u32) {
                Some(part_arc) => {
                    let part_log = part_arc.read();
                    let high_watermark = part_log.log_end_offset();
                    let log_start_offset = part_log.log_start_offset();
                    let fetch_offset = part_target.fetch_offset as u64;

                    if fetch_offset < log_start_offset {
                        PartitionFetchResponse_error(part_id, ErrorCode::OffsetOutOfRange, high_watermark, log_start_offset)
                    } else if fetch_offset >= high_watermark {
                        // Beyond tail: return empty records
                        FetchPartitionResponse::success(part_id, high_watermark, log_start_offset, None)
                    } else {
                        match part_log.read_raw_records_from(fetch_offset, part_target.partition_max_bytes as usize) {
                            Ok((raw_bytes, _next_offset)) => {
                                if raw_bytes.is_empty() {
                                    FetchPartitionResponse::success(part_id, high_watermark, log_start_offset, None)
                                } else {
                                    FetchPartitionResponse::success(
                                        part_id,
                                        high_watermark,
                                        log_start_offset,
                                        Some(raw_bytes),
                                    )
                                }
                            }
                            Err(e) => {
                                warn!("Error reading from {}-{}: {}", topic_target.name, part_id, e);
                                PartitionFetchResponse_error(part_id, ErrorCode::CorruptMessage, high_watermark, log_start_offset)
                            }
                        }
                    }
                }
                None => {
                    PartitionFetchResponse_error(part_id, ErrorCode::UnknownTopicOrPartition, 0, 0)
                }
            };

            partition_responses.push(part_res);
        }

        topic_responses.push(FetchTopicResponse {
            name: topic_target.name,
            partitions: partition_responses,
        });
    }

    let resp = FetchResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None,
        session_id: fetch_req.session_id,
        responses: topic_responses,
    };

    let mut buf = BytesMut::new();
    ResponseHeader::new(req.header.correlation_id).encode(&mut buf);
    resp.encode(req.header.api_version, &mut buf);

    Ok(buf.freeze())
}

#[allow(non_snake_case)]
fn PartitionFetchResponse_error(
    partition_index: i32,
    error_code: ErrorCode,
    high_watermark: u64,
    log_start_offset: u64,
) -> FetchPartitionResponse {
    FetchPartitionResponse {
        partition_index,
        error_code,
        high_watermark: high_watermark as i64,
        last_stable_offset: high_watermark as i64,
        log_start_offset: log_start_offset as i64,
        records: None,
    }
}

/// Helper function to unpack records from incoming produce payload bytes.
fn extract_records(payload: Bytes) -> Vec<Record> {
    let mut records = Vec::new();
    let mut slice: &[u8] = &payload;

    while !slice.is_empty() {
        match Record::decode_from_slice(slice) {
            Ok((record, consumed)) => {
                records.push(record);
                slice = &slice[consumed..];
            }
            Err(_) => break,
        }
    }

    if !records.is_empty() && slice.is_empty() {
        records
    } else {
        // Fallback: preserve payload as an opaque record value
        vec![Record::new(None, Some(payload))]
    }
}
