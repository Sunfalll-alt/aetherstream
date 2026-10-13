//! Fetch API (v4 - v11) request and response codecs.

use bytes::{Buf, BufMut, Bytes};

use crate::error::ProtocolError;
use crate::header::ErrorCode;
use crate::primitives::{
    read_array, read_bytes, read_string, write_array, write_bytes, write_string,
};

/// Partition target in a FetchRequest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPartitionData {
    /// Target partition index.
    pub partition_index: i32,
    /// Current leader epoch (v9+).
    pub current_leader_epoch: Option<i32>,
    /// Logical offset to begin fetching records from.
    pub fetch_offset: i64,
    /// Earliest log start offset known to consumer (v5+).
    pub log_start_offset: i64,
    /// Maximum bytes to return for this partition.
    pub partition_max_bytes: i32,
}

/// Topic target in a FetchRequest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchTopicData {
    /// Topic name.
    pub name: String,
    /// Partition targets list.
    pub partitions: Vec<FetchPartitionData>,
}

/// FetchRequest (v4 - v11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRequest {
    /// Replica node ID initiating the fetch (-1 for normal consumers).
    pub replica_id: i32,
    /// Maximum time in milliseconds the broker should wait for `min_bytes`.
    pub max_wait_ms: i32,
    /// Minimum bytes to accumulate before responding.
    pub min_bytes: i32,
    /// Maximum total bytes across all partitions to return.
    pub max_bytes: i32,
    /// Transaction isolation level (0 = read_uncommitted, 1 = read_committed).
    pub isolation_level: i8,
    /// Session ID (v7+).
    pub session_id: i32,
    /// Session epoch (v7+).
    pub session_epoch: i32,
    /// Topics and partitions to fetch.
    pub topics: Vec<FetchTopicData>,
}

impl FetchRequest {
    /// Creates a single-partition consumer fetch request.
    pub fn new(
        topic: impl Into<String>,
        partition_index: i32,
        fetch_offset: i64,
        max_bytes: i32,
    ) -> Self {
        Self {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 10 * 1024 * 1024,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics: vec![FetchTopicData {
                name: topic.into(),
                partitions: vec![FetchPartitionData {
                    partition_index,
                    current_leader_epoch: None,
                    fetch_offset,
                    log_start_offset: 0,
                    partition_max_bytes: max_bytes,
                }],
            }],
        }
    }

    /// Decodes a FetchRequest according to `version`.
    pub fn decode(version: i16, buf: &mut impl Buf) -> Result<Self, ProtocolError> {
        if buf.remaining() < 17 {
            return Err(ProtocolError::UnexpectedEof {
                expected: 17,
                available: buf.remaining(),
            });
        }

        let replica_id = buf.get_i32();
        let max_wait_ms = buf.get_i32();
        let min_bytes = buf.get_i32();
        let max_bytes = buf.get_i32();
        let isolation_level = buf.get_i8();

        let (session_id, session_epoch) = if version >= 7 {
            if buf.remaining() < 8 {
                return Err(ProtocolError::UnexpectedEof {
                    expected: 8,
                    available: buf.remaining(),
                });
            }
            (buf.get_i32(), buf.get_i32())
        } else {
            (0, -1)
        };

        let topics = read_array(buf, |b| {
            let name = read_string(b)?.unwrap_or_default();
            let partitions = read_array(b, |bp| {
                if bp.remaining() < 16 {
                    return Err(ProtocolError::UnexpectedEof {
                        expected: 16,
                        available: bp.remaining(),
                    });
                }
                let partition_index = bp.get_i32();
                let current_leader_epoch = if version >= 9 {
                    Some(bp.get_i32())
                } else {
                    None
                };
                let fetch_offset = bp.get_i64();
                let log_start_offset = if version >= 5 {
                    if bp.remaining() < 8 {
                        return Err(ProtocolError::UnexpectedEof {
                            expected: 8,
                            available: bp.remaining(),
                        });
                    }
                    bp.get_i64()
                } else {
                    0
                };
                if bp.remaining() < 4 {
                    return Err(ProtocolError::UnexpectedEof {
                        expected: 4,
                        available: bp.remaining(),
                    });
                }
                let partition_max_bytes = bp.get_i32();

                Ok(FetchPartitionData {
                    partition_index,
                    current_leader_epoch,
                    fetch_offset,
                    log_start_offset,
                    partition_max_bytes,
                })
            })?
            .unwrap_or_default();

            Ok(FetchTopicData { name, partitions })
        })?
        .unwrap_or_default();

        Ok(Self {
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
        })
    }

    /// Encodes a FetchRequest according to `version`.
    pub fn encode(&self, version: i16, buf: &mut impl BufMut) {
        buf.put_i32(self.replica_id);
        buf.put_i32(self.max_wait_ms);
        buf.put_i32(self.min_bytes);
        buf.put_i32(self.max_bytes);
        buf.put_i8(self.isolation_level);

        if version >= 7 {
            buf.put_i32(self.session_id);
            buf.put_i32(self.session_epoch);
        }

        write_array(buf, Some(&self.topics), |b, t| {
            write_string(b, Some(&t.name));
            write_array(b, Some(&t.partitions), |bp, p| {
                bp.put_i32(p.partition_index);
                if version >= 9 {
                    bp.put_i32(p.current_leader_epoch.unwrap_or(-1));
                }
                bp.put_i64(p.fetch_offset);
                if version >= 5 {
                    bp.put_i64(p.log_start_offset);
                }
                bp.put_i32(p.partition_max_bytes);
            });
        });
    }
}

/// Partition records result within a FetchResponse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPartitionResponse {
    /// Partition index.
    pub partition_index: i32,
    /// Partition error code.
    pub error_code: ErrorCode,
    /// High watermark offset in partition log.
    pub high_watermark: i64,
    /// Last stable offset for transactional isolation (v4+).
    pub last_stable_offset: i64,
    /// Earliest log start offset (v5+).
    pub log_start_offset: i64,
    /// Returned record batch bytes.
    pub records: Option<Bytes>,
}

impl FetchPartitionResponse {
    /// Creates a successful partition fetch result with record bytes.
    pub fn success(
        partition_index: i32,
        high_watermark: u64,
        log_start_offset: u64,
        records: Option<Bytes>,
    ) -> Self {
        Self {
            partition_index,
            error_code: ErrorCode::None,
            high_watermark: high_watermark as i64,
            last_stable_offset: high_watermark as i64,
            log_start_offset: log_start_offset as i64,
            records,
        }
    }
}

/// Topic result within a FetchResponse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchTopicResponse {
    /// Topic name.
    pub name: String,
    /// Partition fetch results.
    pub partitions: Vec<FetchPartitionResponse>,
}

/// FetchResponse (v4 - v11).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FetchResponse {
    /// Broker throttle duration in milliseconds (v1+).
    pub throttle_time_ms: i32,
    /// Top-level error code (v7+).
    pub error_code: ErrorCode,
    /// Session ID (v7+).
    pub session_id: i32,
    /// Topics fetch results.
    pub responses: Vec<FetchTopicResponse>,
}

impl FetchResponse {
    /// Creates a single-topic partition fetch response.
    pub fn single(
        topic: impl Into<String>,
        partition_index: i32,
        high_watermark: u64,
        log_start_offset: u64,
        records: Option<Bytes>,
    ) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
            session_id: 0,
            responses: vec![FetchTopicResponse {
                name: topic.into(),
                partitions: vec![FetchPartitionResponse::success(
                    partition_index,
                    high_watermark,
                    log_start_offset,
                    records,
                )],
            }],
        }
    }

    /// Decodes a FetchResponse according to `version`.
    pub fn decode(version: i16, buf: &mut impl Buf) -> Result<Self, ProtocolError> {
        let throttle_time_ms = if buf.remaining() >= 4 {
            buf.get_i32()
        } else {
            0
        };

        let (error_code, session_id) = if version >= 7 {
            if buf.remaining() < 6 {
                return Err(ProtocolError::UnexpectedEof {
                    expected: 6,
                    available: buf.remaining(),
                });
            }
            (ErrorCode::from_i16(buf.get_i16()), buf.get_i32())
        } else {
            (ErrorCode::None, 0)
        };

        let responses = read_array(buf, |b| {
            let name = read_string(b)?.unwrap_or_default();
            let partitions = read_array(b, |bp| {
                if bp.remaining() < 14 {
                    return Err(ProtocolError::UnexpectedEof {
                        expected: 14,
                        available: bp.remaining(),
                    });
                }
                let partition_index = bp.get_i32();
                let error_code = ErrorCode::from_i16(bp.get_i16());
                let high_watermark = bp.get_i64();

                let last_stable_offset = if version >= 4 {
                    if bp.remaining() < 8 {
                        return Err(ProtocolError::UnexpectedEof {
                            expected: 8,
                            available: bp.remaining(),
                        });
                    }
                    bp.get_i64()
                } else {
                    high_watermark
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
                    0
                };

                // In v4+, skip aborted_transactions array
                if version >= 4 {
                    let _ = read_array(bp, |b_tx| {
                        if b_tx.remaining() < 16 {
                            return Err(ProtocolError::UnexpectedEof {
                                expected: 16,
                                available: b_tx.remaining(),
                            });
                        }
                        b_tx.advance(16);
                        Ok(())
                    })?;
                }

                // In v11+, preferred_read_replica: i32
                if version >= 11 {
                    if bp.remaining() < 4 {
                        return Err(ProtocolError::UnexpectedEof {
                            expected: 4,
                            available: bp.remaining(),
                        });
                    }
                    bp.advance(4);
                }

                let records = read_bytes(bp)?;

                Ok(FetchPartitionResponse {
                    partition_index,
                    error_code,
                    high_watermark,
                    last_stable_offset,
                    log_start_offset,
                    records,
                })
            })?
            .unwrap_or_default();

            Ok(FetchTopicResponse { name, partitions })
        })?
        .unwrap_or_default();

        Ok(Self {
            throttle_time_ms,
            error_code,
            session_id,
            responses,
        })
    }

    /// Encodes a FetchResponse according to `version`.
    pub fn encode(&self, version: i16, buf: &mut impl BufMut) {
        buf.put_i32(self.throttle_time_ms);

        if version >= 7 {
            buf.put_i16(self.error_code.as_i16());
            buf.put_i32(self.session_id);
        }

        write_array(buf, Some(&self.responses), |b, t| {
            write_string(b, Some(&t.name));
            write_array(b, Some(&t.partitions), |bp, p| {
                bp.put_i32(p.partition_index);
                bp.put_i16(p.error_code.as_i16());
                bp.put_i64(p.high_watermark);
                if version >= 4 {
                    bp.put_i64(p.last_stable_offset);
                }
                if version >= 5 {
                    bp.put_i64(p.log_start_offset);
                }
                if version >= 4 {
                    // Empty aborted transactions array (-1)
                    bp.put_i32(-1);
                }
                if version >= 11 {
                    // Preferred read replica (-1 for none)
                    bp.put_i32(-1);
                }
                write_bytes(bp, p.records.as_deref());
            });
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_fetch_request_roundtrip() {
        let req = FetchRequest::new("sensors", 0, 42, 65536);

        for v in 4..=11 {
            let mut buf = BytesMut::new();
            req.encode(v, &mut buf);

            let mut cursor = buf.freeze();
            let decoded =
                FetchRequest::decode(v, &mut cursor).expect("Decode fetch request failed");
            assert_eq!(decoded.topics.len(), 1);
            assert_eq!(decoded.topics[0].name, "sensors");
            assert_eq!(decoded.topics[0].partitions[0].fetch_offset, 42);
            assert_eq!(decoded.topics[0].partitions[0].partition_max_bytes, 65536);
        }
    }

    #[test]
    fn test_fetch_response_roundtrip() {
        let resp = FetchResponse::single(
            "sensors",
            0,
            100,
            0,
            Some(Bytes::from_static(b"stream-data-payload")),
        );

        for v in 4..=11 {
            let mut buf = BytesMut::new();
            resp.encode(v, &mut buf);

            let mut cursor = buf.freeze();
            let decoded =
                FetchResponse::decode(v, &mut cursor).expect("Decode fetch response failed");
            assert_eq!(decoded.responses.len(), 1);
            assert_eq!(decoded.responses[0].name, "sensors");
            assert_eq!(decoded.responses[0].partitions[0].high_watermark, 100);
            assert_eq!(
                decoded.responses[0].partitions[0].records,
                Some(Bytes::from_static(b"stream-data-payload"))
            );
        }
    }
}
