//! Produce API (v3 - v8) request and response codecs.

use bytes::{Buf, BufMut, Bytes};

use crate::error::ProtocolError;
use crate::header::ErrorCode;
use crate::primitives::{read_array, read_bytes, read_string, write_array, write_bytes, write_string};

/// Data for a single partition within a ProduceRequest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionProduceData {
    /// Target partition index.
    pub partition_index: i32,
    /// Serialized record batch payload.
    pub records: Option<Bytes>,
}

/// Data for a single topic within a ProduceRequest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicProduceData {
    /// Topic name.
    pub name: String,
    /// Partition payload list.
    pub partition_data: Vec<PartitionProduceData>,
}

/// ProduceRequest (v3 - v8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequest {
    /// Transactional ID string (v3+, null for non-transactional).
    pub transactional_id: Option<String>,
    /// Required acknowledgment level: 0 = none, 1 = leader, -1 = all.
    pub acks: i16,
    /// Maximum time in milliseconds the broker can wait for acks.
    pub timeout_ms: i32,
    /// Topic record batches.
    pub topic_data: Vec<TopicProduceData>,
}

impl ProduceRequest {
    /// Creates a single-topic produce request.
    pub fn new(
        topic: impl Into<String>,
        partition_index: i32,
        records: Bytes,
        acks: i16,
    ) -> Self {
        Self {
            transactional_id: None,
            acks,
            timeout_ms: 5000,
            topic_data: vec![TopicProduceData {
                name: topic.into(),
                partition_data: vec![PartitionProduceData {
                    partition_index,
                    records: Some(records),
                }],
            }],
        }
    }

    /// Decodes a ProduceRequest from the buffer according to `version`.
    pub fn decode(version: i16, buf: &mut impl Buf) -> Result<Self, ProtocolError> {
        let transactional_id = if version >= 3 {
            read_string(buf)?
        } else {
            None
        };

        if buf.remaining() < 6 {
            return Err(ProtocolError::UnexpectedEof {
                expected: 6,
                available: buf.remaining(),
            });
        }

        let acks = buf.get_i16();
        let timeout_ms = buf.get_i32();

        let topic_data = read_array(buf, |b| {
            let name = read_string(b)?.unwrap_or_default();
            let partition_data = read_array(b, |bp| {
                if bp.remaining() < 4 {
                    return Err(ProtocolError::UnexpectedEof {
                        expected: 4,
                        available: bp.remaining(),
                    });
                }
                let partition_index = bp.get_i32();
                let records = read_bytes(bp)?;
                Ok(PartitionProduceData {
                    partition_index,
                    records,
                })
            })?
            .unwrap_or_default();

            Ok(TopicProduceData {
                name,
                partition_data,
            })
        })?
        .unwrap_or_default();

        Ok(Self {
            transactional_id,
            acks,
            timeout_ms,
            topic_data,
        })
    }

    /// Encodes a ProduceRequest into the buffer according to `version`.
    pub fn encode(&self, version: i16, buf: &mut impl BufMut) {
        if version >= 3 {
            write_string(buf, self.transactional_id.as_deref());
        }
        buf.put_i16(self.acks);
        buf.put_i32(self.timeout_ms);

        write_array(buf, Some(&self.topic_data), |b, t| {
            write_string(b, Some(&t.name));
            write_array(b, Some(&t.partition_data), |bp, p| {
                bp.put_i32(p.partition_index);
                write_bytes(bp, p.records.as_deref());
            });
        });
    }
}

/// Partition result within a ProduceResponse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionProduceResponse {
    /// Partition index.
    pub partition_index: i32,
    /// Partition error code.
    pub error_code: ErrorCode,
    /// First assigned logical log offset for the appended batch.
    pub base_offset: i64,
    /// Timestamp assigned to the appended record batch (v2+).
    pub log_append_time_ms: i64,
    /// Current log start offset after append (v5+).
    pub log_start_offset: i64,
}

impl PartitionProduceResponse {
    /// Creates a successful partition append response.
    pub fn success(partition_index: i32, base_offset: u64, log_start_offset: u64) -> Self {
        Self {
            partition_index,
            error_code: ErrorCode::None,
            base_offset: base_offset as i64,
            log_append_time_ms: -1,
            log_start_offset: log_start_offset as i64,
        }
    }

    /// Creates an error partition response.
    pub fn error(partition_index: i32, error_code: ErrorCode) -> Self {
        Self {
            partition_index,
            error_code,
            base_offset: -1,
            log_append_time_ms: -1,
            log_start_offset: -1,
        }
    }
}

/// Topic result within a ProduceResponse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicProduceResponse {
    /// Topic name.
    pub name: String,
    /// Partition results list.
    pub partition_responses: Vec<PartitionProduceResponse>,
}

/// ProduceResponse (v3 - v8).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProduceResponse {
    /// Per-topic partition append outcomes.
    pub responses: Vec<TopicProduceResponse>,
    /// Broker throttle duration in milliseconds (v1+).
    pub throttle_time_ms: i32,
}

impl ProduceResponse {
    /// Creates a single-partition produce response.
    pub fn single(
        topic: impl Into<String>,
        partition_index: i32,
        base_offset: u64,
        log_start_offset: u64,
    ) -> Self {
        Self {
            responses: vec![TopicProduceResponse {
                name: topic.into(),
                partition_responses: vec![PartitionProduceResponse::success(
                    partition_index,
                    base_offset,
                    log_start_offset,
                )],
            }],
            throttle_time_ms: 0,
        }
    }

    /// Decodes a ProduceResponse from the buffer according to `version`.
    pub fn decode(version: i16, buf: &mut impl Buf) -> Result<Self, ProtocolError> {
        let responses = read_array(buf, |b| {
            let name = read_string(b)?.unwrap_or_default();
            let partition_responses = read_array(b, |bp| {
                if bp.remaining() < 14 {
                    return Err(ProtocolError::UnexpectedEof {
                        expected: 14,
                        available: bp.remaining(),
                    });
                }
                let partition_index = bp.get_i32();
                let error_code = ErrorCode::from_i16(bp.get_i16());
                let base_offset = bp.get_i64();

                let log_append_time_ms = if version >= 2 {
                    if bp.remaining() < 8 {
                        return Err(ProtocolError::UnexpectedEof {
                            expected: 8,
                            available: bp.remaining(),
                        });
                    }
                    bp.get_i64()
                } else {
                    -1
                };

                let log_start_offset = if version >= 5 {
                    if bp.remaining() < 8 {
                        return Err(ProtocolError::UnexpectedEof {
                            expected: 8,
                            available: bp.remaining(),
                        });
                    }
                    bp.get_i64()
                } else {
                    -1
                };

                Ok(PartitionProduceResponse {
                    partition_index,
                    error_code,
                    base_offset,
                    log_append_time_ms,
                    log_start_offset,
                })
            })?
            .unwrap_or_default();

            Ok(TopicProduceResponse {
                name,
                partition_responses,
            })
        })?
        .unwrap_or_default();

        let throttle_time_ms = if version >= 1 {
            if buf.remaining() < 4 {
                return Err(ProtocolError::UnexpectedEof {
                    expected: 4,
                    available: buf.remaining(),
                });
            }
            buf.get_i32()
        } else {
            0
        };

        Ok(Self {
            responses,
            throttle_time_ms,
        })
    }

    /// Encodes a ProduceResponse into the buffer according to `version`.
    pub fn encode(&self, version: i16, buf: &mut impl BufMut) {
        write_array(buf, Some(&self.responses), |b, t| {
            write_string(b, Some(&t.name));
            write_array(b, Some(&t.partition_responses), |bp, p| {
                bp.put_i32(p.partition_index);
                bp.put_i16(p.error_code.as_i16());
                bp.put_i64(p.base_offset);
                if version >= 2 {
                    bp.put_i64(p.log_append_time_ms);
                }
                if version >= 5 {
                    bp.put_i64(p.log_start_offset);
                }
            });
        });

        if version >= 1 {
            buf.put_i32(self.throttle_time_ms);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_produce_request_roundtrip() {
        let req = ProduceRequest::new(
            "metrics",
            0,
            Bytes::from_static(b"sample-record-batch-data"),
            1,
        );

        for v in 3..=8 {
            let mut buf = BytesMut::new();
            req.encode(v, &mut buf);

            let mut cursor = buf.freeze();
            let decoded = ProduceRequest::decode(v, &mut cursor).expect("Decode produce request failed");
            assert_eq!(decoded.acks, 1);
            assert_eq!(decoded.topic_data.len(), 1);
            assert_eq!(decoded.topic_data[0].name, "metrics");
            assert_eq!(decoded.topic_data[0].partition_data.len(), 1);
            assert_eq!(
                decoded.topic_data[0].partition_data[0].records,
                Some(Bytes::from_static(b"sample-record-batch-data"))
            );
        }
    }

    #[test]
    fn test_produce_response_roundtrip() {
        let resp = ProduceResponse::single("metrics", 0, 105, 0);

        for v in 3..=8 {
            let mut buf = BytesMut::new();
            resp.encode(v, &mut buf);

            let mut cursor = buf.freeze();
            let decoded = ProduceResponse::decode(v, &mut cursor).expect("Decode produce response failed");
            assert_eq!(decoded.responses.len(), 1);
            assert_eq!(decoded.responses[0].name, "metrics");
            assert_eq!(
                decoded.responses[0].partition_responses[0].base_offset,
                105
            );
            assert_eq!(
                decoded.responses[0].partition_responses[0].error_code,
                ErrorCode::None
            );
        }
    }
}
