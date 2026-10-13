//! # AetherStream Protocol Layer
//!
//! High-performance, zero-JVM implementation of the Apache Kafka binary wire protocol.
//!
//! ## Core Architecture:
//! - **Framing**: `KafkaCodec` implementing 4-byte big-endian framing via `tokio_util::codec::{Decoder, Encoder}`.
//! - **Headers**: Strongly-typed `RequestHeader` (v0-v2) and `ResponseHeader` (v0-v1).
//! - **Primitives**: Zero-copy encoders and decoders for Kafka scalar types, varints, strings, bytes, and tagged fields.
//! - **Message APIs**:
//!   - `ApiVersions` (v0-v3)
//!   - `Metadata` (v0-v7)
//!   - `Produce` (v3-v8)
//!   - `Fetch` (v4-v11)

#![warn(missing_docs)]

pub mod error;
pub mod frame;
pub mod header;
pub mod messages;
pub mod primitives;

pub use error::ProtocolError;
pub use frame::{InboundRequest, KafkaCodec, DEFAULT_MAX_FRAME_BYTES};
pub use header::{ApiKey, ErrorCode, RequestHeader, ResponseHeader};
pub use messages::{
    ApiVersionSupport, ApiVersionsRequest, ApiVersionsResponse, BrokerMetadata, FetchPartitionData,
    FetchPartitionResponse, FetchRequest, FetchResponse, FetchTopicData, FetchTopicResponse,
    MetadataRequest, MetadataResponse, PartitionMetadata, PartitionProduceData,
    PartitionProduceResponse, ProduceRequest, ProduceResponse, TopicMetadata, TopicProduceData,
    TopicProduceResponse,
};
