//! Kafka Request and Response headers and API key definitions.

use bytes::{Buf, BufMut};

use crate::error::ProtocolError;
use crate::primitives::{read_string, skip_tagged_fields, write_empty_tagged_fields, write_string};

/// Supported Apache Kafka API Keys.
#[repr(i16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ApiKey {
    /// Produce API (Key 0)
    Produce = 0,
    /// Fetch API (Key 1)
    Fetch = 1,
    /// ListOffsets API (Key 2)
    ListOffsets = 2,
    /// Metadata API (Key 3)
    Metadata = 3,
    /// OffsetCommit API (Key 8)
    OffsetCommit = 8,
    /// OffsetFetch API (Key 9)
    OffsetFetch = 9,
    /// FindCoordinator API (Key 10)
    FindCoordinator = 10,
    /// JoinGroup API (Key 11)
    JoinGroup = 11,
    /// Heartbeat API (Key 12)
    Heartbeat = 12,
    /// LeaveGroup API (Key 13)
    LeaveGroup = 13,
    /// SyncGroup API (Key 14)
    SyncGroup = 14,
    /// DescribeGroups API (Key 15)
    DescribeGroups = 15,
    /// ListGroups API (Key 16)
    ListGroups = 16,
    /// ApiVersions API (Key 18)
    ApiVersions = 18,
}

impl ApiKey {
    /// Converts a raw signed 16-bit integer into a strongly-typed `ApiKey`.
    pub fn from_i16(val: i16) -> Result<Self, ProtocolError> {
        match val {
            0 => Ok(Self::Produce),
            1 => Ok(Self::Fetch),
            2 => Ok(Self::ListOffsets),
            3 => Ok(Self::Metadata),
            8 => Ok(Self::OffsetCommit),
            9 => Ok(Self::OffsetFetch),
            10 => Ok(Self::FindCoordinator),
            11 => Ok(Self::JoinGroup),
            12 => Ok(Self::Heartbeat),
            13 => Ok(Self::LeaveGroup),
            14 => Ok(Self::SyncGroup),
            15 => Ok(Self::DescribeGroups),
            16 => Ok(Self::ListGroups),
            18 => Ok(Self::ApiVersions),
            other => Err(ProtocolError::UnsupportedApiKey(other)),
        }
    }

    /// Returns the integer API key value.
    #[inline]
    pub const fn as_i16(self) -> i16 {
        self as i16
    }
}

/// Standard Kafka Error Codes.
#[repr(i16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ErrorCode {
    /// No error (0).
    #[default]
    None = 0,
    /// Offset out of range (1).
    OffsetOutOfRange = 1,
    /// Corrupt message (2).
    CorruptMessage = 2,
    /// Unknown topic or partition (3).
    UnknownTopicOrPartition = 3,
    /// Leader not available (5).
    LeaderNotAvailable = 5,
    /// Not leader or follower for partition (6).
    NotLeaderOrFollower = 6,
    /// Request timed out (7).
    RequestTimedOut = 7,
    /// Broker not available (8).
    BrokerNotAvailable = 8,
    /// Replica not available (9).
    ReplicaNotAvailable = 9,
    /// Message too large (10).
    MessageTooLarge = 10,
    /// Invalid topic exception (17).
    InvalidTopicException = 17,
    /// Record list too large (18).
    RecordListTooLarge = 18,
    /// Not enough in-sync replicas (19).
    NotEnoughReplicas = 19,
    /// Not enough replicas after append (20).
    NotEnoughReplicasAfterAppend = 20,
    /// Invalid required acks (21).
    InvalidRequiredAcks = 21,
    /// Unsupported version (35).
    UnsupportedVersion = 35,
    /// Topic already exists (36).
    TopicAlreadyExists = 36,
    /// Unknown error.
    Unknown = -1,
}

impl ErrorCode {
    /// Converts a raw i16 to an `ErrorCode`.
    pub fn from_i16(val: i16) -> Self {
        match val {
            0 => Self::None,
            1 => Self::OffsetOutOfRange,
            2 => Self::CorruptMessage,
            3 => Self::UnknownTopicOrPartition,
            5 => Self::LeaderNotAvailable,
            6 => Self::NotLeaderOrFollower,
            7 => Self::RequestTimedOut,
            8 => Self::BrokerNotAvailable,
            9 => Self::ReplicaNotAvailable,
            10 => Self::MessageTooLarge,
            17 => Self::InvalidTopicException,
            18 => Self::RecordListTooLarge,
            19 => Self::NotEnoughReplicas,
            20 => Self::NotEnoughReplicasAfterAppend,
            21 => Self::InvalidRequiredAcks,
            35 => Self::UnsupportedVersion,
            36 => Self::TopicAlreadyExists,
            _ => Self::Unknown,
        }
    }

    /// Returns the integer code value.
    #[inline]
    pub const fn as_i16(self) -> i16 {
        self as i16
    }
}

/// Kafka Request Header (v0 - v2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHeader {
    /// Target API key.
    pub api_key: ApiKey,
    /// Client requested API version.
    pub api_version: i16,
    /// Request correlation ID assigned by client.
    pub correlation_id: i32,
    /// Client identifier string (v1+).
    pub client_id: Option<String>,
}

impl RequestHeader {
    /// Decodes a RequestHeader from the incoming framed buffer.
    ///
    /// Handles v0 (no client_id), v1 (with client_id), and v2 (flexible tagged fields).
    pub fn decode(buf: &mut impl Buf) -> Result<Self, ProtocolError> {
        if buf.remaining() < 8 {
            return Err(ProtocolError::UnexpectedEof {
                expected: 8,
                available: buf.remaining(),
            });
        }

        let api_key_raw = buf.get_i16();
        let api_key = ApiKey::from_i16(api_key_raw)?;
        let api_version = buf.get_i16();
        let correlation_id = buf.get_i32();

        // If buffer has remaining bytes, decode client_id (v1+)
        let client_id = if buf.has_remaining() {
            read_string(buf)?
        } else {
            None
        };

        // For flexible versions (v2 header), skip tagged fields if present
        // (ApiVersions v3 uses header v2)
        if api_key == ApiKey::ApiVersions && api_version >= 3 && buf.has_remaining() {
            let _ = skip_tagged_fields(buf);
        }

        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }

    /// Encodes this request header into a buffer (v1 format by default).
    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_i16(self.api_key.as_i16());
        buf.put_i16(self.api_version);
        buf.put_i32(self.correlation_id);
        write_string(buf, self.client_id.as_deref());
    }
}

/// Kafka Response Header (v0 - v1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseHeader {
    /// Correlation ID copied from the matching request.
    pub correlation_id: i32,
}

impl ResponseHeader {
    /// Creates a response header with the given correlation ID.
    pub fn new(correlation_id: i32) -> Self {
        Self { correlation_id }
    }

    /// Decodes a ResponseHeader from the buffer.
    pub fn decode(buf: &mut impl Buf) -> Result<Self, ProtocolError> {
        if buf.remaining() < 4 {
            return Err(ProtocolError::UnexpectedEof {
                expected: 4,
                available: buf.remaining(),
            });
        }
        let correlation_id = buf.get_i32();
        Ok(Self { correlation_id })
    }

    /// Encodes a ResponseHeader (v0: standard 4-byte correlation ID).
    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_i32(self.correlation_id);
    }

    /// Encodes a ResponseHeader with flexible tagged fields (v1: 4-byte correlation ID + empty tags).
    pub fn encode_v1(&self, buf: &mut impl BufMut) {
        buf.put_i32(self.correlation_id);
        write_empty_tagged_fields(buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_request_header_v1_roundtrip() {
        let header = RequestHeader {
            api_key: ApiKey::Metadata,
            api_version: 5,
            correlation_id: 12345,
            client_id: Some("console-producer".to_string()),
        };

        let mut buf = BytesMut::new();
        header.encode(&mut buf);

        let mut cursor = buf.freeze();
        let decoded = RequestHeader::decode(&mut cursor).expect("Decode should succeed");
        assert_eq!(decoded, header);
        assert_eq!(cursor.remaining(), 0);
    }

    #[test]
    fn test_request_header_v0_roundtrip() {
        let mut buf = BytesMut::new();
        buf.put_i16(ApiKey::ApiVersions.as_i16());
        buf.put_i16(0);
        buf.put_i32(42);

        let mut cursor = buf.freeze();
        let decoded = RequestHeader::decode(&mut cursor).unwrap();
        assert_eq!(decoded.api_key, ApiKey::ApiVersions);
        assert_eq!(decoded.api_version, 0);
        assert_eq!(decoded.correlation_id, 42);
        assert_eq!(decoded.client_id, None);
    }

    #[test]
    fn test_response_header_roundtrip() {
        let resp_header = ResponseHeader::new(98765);
        let mut buf = BytesMut::new();
        resp_header.encode(&mut buf);

        let mut cursor = buf.freeze();
        let decoded = ResponseHeader::decode(&mut cursor).unwrap();
        assert_eq!(decoded.correlation_id, 98765);
    }
}
