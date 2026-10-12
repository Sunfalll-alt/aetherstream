//! Metadata API (v0 - v7) request and response codecs.

use bytes::{Buf, BufMut};

use crate::error::ProtocolError;
use crate::header::ErrorCode;
use crate::primitives::{read_array, read_string, write_array, write_string};

/// MetadataRequest (v0 - v7).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MetadataRequest {
    /// List of topic names requested, or `None` if all topics are requested.
    pub topics: Option<Vec<String>>,
    /// Whether to permit auto topic creation if topics do not yet exist (v4+).
    pub allow_auto_topic_creation: bool,
}

impl MetadataRequest {
    /// Creates a request asking for all cluster topics.
    pub fn all_topics() -> Self {
        Self {
            topics: None,
            allow_auto_topic_creation: true,
        }
    }

    /// Creates a request asking for specific topic names.
    pub fn for_topics(topics: Vec<String>) -> Self {
        Self {
            topics: Some(topics),
            allow_auto_topic_creation: true,
        }
    }

    /// Decodes a MetadataRequest from the incoming buffer according to version.
    pub fn decode(version: i16, buf: &mut impl Buf) -> Result<Self, ProtocolError> {
        let topics = if version == 0 {
            // In v0, empty array means ALL topics
            let parsed = read_array(buf, |b| read_string(b).map(|s| s.unwrap_or_default()))?;
            match parsed {
                Some(vec) if vec.is_empty() => None,
                Some(vec) => Some(vec),
                None => None,
            }
        } else {
            // In v1+, null array (-1) means ALL topics
            read_array(buf, |b| read_string(b).map(|s| s.unwrap_or_default()))?
        };

        let allow_auto_topic_creation = if version >= 4 && buf.has_remaining() {
            buf.get_u8() != 0
        } else {
            true
        };

        Ok(Self {
            topics,
            allow_auto_topic_creation,
        })
    }

    /// Encodes a MetadataRequest into the buffer.
    pub fn encode(&self, version: i16, buf: &mut impl BufMut) {
        if version == 0 {
            let empty: &[String] = &[];
            let slice = self.topics.as_deref().unwrap_or(empty);
            write_array(buf, Some(slice), |b, s| write_string(b, Some(s)));
        } else {
            write_array(buf, self.topics.as_deref(), |b, s| write_string(b, Some(s)));
        }

        if version >= 4 {
            buf.put_u8(if self.allow_auto_topic_creation { 1 } else { 0 });
        }
    }
}

/// Metadata describing an individual cluster broker node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerMetadata {
    /// Node ID.
    pub node_id: i32,
    /// Broker network hostname or IP.
    pub host: String,
    /// Broker listener port.
    pub port: i32,
    /// Rack designation (v1+).
    pub rack: Option<String>,
}

impl BrokerMetadata {
    /// Creates a new broker metadata descriptor.
    pub fn new(node_id: i32, host: impl Into<String>, port: i32) -> Self {
        Self {
            node_id,
            host: host.into(),
            port,
            rack: None,
        }
    }
}

/// Metadata describing an individual topic partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    /// Partition-level error code.
    pub error_code: ErrorCode,
    /// Partition index number.
    pub partition_index: i32,
    /// Broker node ID acting as the current partition leader.
    pub leader_id: i32,
    /// Partition leader epoch (v7+).
    pub leader_epoch: Option<i32>,
    /// Set of replica broker IDs.
    pub replica_nodes: Vec<i32>,
    /// Set of in-sync replica (ISR) broker IDs.
    pub isr_nodes: Vec<i32>,
    /// Set of offline replica IDs (v5+).
    pub offline_replicas: Vec<i32>,
}

impl PartitionMetadata {
    /// Creates a healthy partition descriptor assigned to a specific leader broker.
    pub fn leader(partition_index: i32, leader_id: i32) -> Self {
        Self {
            error_code: ErrorCode::None,
            partition_index,
            leader_id,
            leader_epoch: Some(0),
            replica_nodes: vec![leader_id],
            isr_nodes: vec![leader_id],
            offline_replicas: Vec::new(),
        }
    }
}

/// Metadata describing a topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    /// Topic-level error code.
    pub error_code: ErrorCode,
    /// Topic name string.
    pub name: String,
    /// Whether this is an internal system topic (e.g. `__consumer_offsets`) (v1+).
    pub is_internal: bool,
    /// Partition descriptors for this topic.
    pub partitions: Vec<PartitionMetadata>,
}

impl TopicMetadata {
    /// Creates a healthy topic metadata descriptor with the given partitions.
    pub fn success(name: impl Into<String>, partitions: Vec<PartitionMetadata>) -> Self {
        Self {
            error_code: ErrorCode::None,
            name: name.into(),
            is_internal: false,
            partitions,
        }
    }
}

/// MetadataResponse (v0 - v7).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MetadataResponse {
    /// Throttle time in milliseconds (v3+).
    pub throttle_time_ms: i32,
    /// Brokers currently in the cluster.
    pub brokers: Vec<BrokerMetadata>,
    /// Cluster ID string identifier (v2+).
    pub cluster_id: Option<String>,
    /// Broker node ID of the active cluster controller (v1+).
    pub controller_id: i32,
    /// Topic metadata descriptors.
    pub topics: Vec<TopicMetadata>,
}

impl MetadataResponse {
    /// Helper to generate a single-node broker metadata response for a set of topics.
    pub fn single_broker(
        node_id: i32,
        host: &str,
        port: i32,
        topics: Vec<TopicMetadata>,
    ) -> Self {
        Self {
            throttle_time_ms: 0,
            brokers: vec![BrokerMetadata::new(node_id, host, port)],
            cluster_id: Some("aether-cluster-local".to_string()),
            controller_id: node_id,
            topics,
        }
    }

    /// Decodes a MetadataResponse according to `version`.
    pub fn decode(version: i16, buf: &mut impl Buf) -> Result<Self, ProtocolError> {
        let throttle_time_ms = if version >= 3 {
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

        // Brokers
        let brokers = read_array(buf, |b| {
            if b.remaining() < 4 {
                return Err(ProtocolError::UnexpectedEof {
                    expected: 4,
                    available: b.remaining(),
                });
            }
            let node_id = b.get_i32();
            let host = read_string(b)?.unwrap_or_default();
            if b.remaining() < 4 {
                return Err(ProtocolError::UnexpectedEof {
                    expected: 4,
                    available: b.remaining(),
                });
            }
            let port = b.get_i32();
            let rack = if version >= 1 {
                read_string(b)?
            } else {
                None
            };
            Ok(BrokerMetadata {
                node_id,
                host,
                port,
                rack,
            })
        })?
        .unwrap_or_default();

        let cluster_id = if version >= 2 {
            read_string(buf)?
        } else {
            None
        };

        let controller_id = if version >= 1 {
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

        // Topics
        let topics = read_array(buf, |b| {
            if b.remaining() < 2 {
                return Err(ProtocolError::UnexpectedEof {
                    expected: 2,
                    available: b.remaining(),
                });
            }
            let error_code = ErrorCode::from_i16(b.get_i16());
            let name = read_string(b)?.unwrap_or_default();
            let is_internal = if version >= 1 {
                if !b.has_remaining() {
                    return Err(ProtocolError::UnexpectedEof {
                        expected: 1,
                        available: 0,
                    });
                }
                b.get_u8() != 0
            } else {
                false
            };

            let partitions = read_array(b, |bp| {
                if bp.remaining() < 10 {
                    return Err(ProtocolError::UnexpectedEof {
                        expected: 10,
                        available: bp.remaining(),
                    });
                }
                let part_err = ErrorCode::from_i16(bp.get_i16());
                let partition_index = bp.get_i32();
                let leader_id = bp.get_i32();
                let leader_epoch = if version >= 7 {
                    if bp.remaining() < 4 {
                        return Err(ProtocolError::UnexpectedEof {
                            expected: 4,
                            available: bp.remaining(),
                        });
                    }
                    Some(bp.get_i32())
                } else {
                    None
                };

                let replica_nodes = read_array(bp, |br| {
                    if br.remaining() < 4 {
                        return Err(ProtocolError::UnexpectedEof {
                            expected: 4,
                            available: br.remaining(),
                        });
                    }
                    Ok(br.get_i32())
                })?
                .unwrap_or_default();

                let isr_nodes = read_array(bp, |bi| {
                    if bi.remaining() < 4 {
                        return Err(ProtocolError::UnexpectedEof {
                            expected: 4,
                            available: bi.remaining(),
                        });
                    }
                    Ok(bi.get_i32())
                })?
                .unwrap_or_default();

                let offline_replicas = if version >= 5 {
                    read_array(bp, |bo| {
                        if bo.remaining() < 4 {
                            return Err(ProtocolError::UnexpectedEof {
                                expected: 4,
                                available: bo.remaining(),
                            });
                        }
                        Ok(bo.get_i32())
                    })?
                    .unwrap_or_default()
                } else {
                    Vec::new()
                };

                Ok(PartitionMetadata {
                    error_code: part_err,
                    partition_index,
                    leader_id,
                    leader_epoch,
                    replica_nodes,
                    isr_nodes,
                    offline_replicas,
                })
            })?
            .unwrap_or_default();

            Ok(TopicMetadata {
                error_code,
                name,
                is_internal,
                partitions,
            })
        })?
        .unwrap_or_default();

        Ok(Self {
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
        })
    }

    /// Encodes a MetadataResponse according to `version`.
    pub fn encode(&self, version: i16, buf: &mut impl BufMut) {
        if version >= 3 {
            buf.put_i32(self.throttle_time_ms);
        }

        // Brokers
        write_array(buf, Some(&self.brokers), |b, broker| {
            b.put_i32(broker.node_id);
            write_string(b, Some(&broker.host));
            b.put_i32(broker.port);
            if version >= 1 {
                write_string(b, broker.rack.as_deref());
            }
        });

        // Cluster ID (v2+)
        if version >= 2 {
            write_string(buf, self.cluster_id.as_deref());
        }

        // Controller ID (v1+)
        if version >= 1 {
            buf.put_i32(self.controller_id);
        }

        // Topics
        write_array(buf, Some(&self.topics), |b, topic| {
            b.put_i16(topic.error_code.as_i16());
            write_string(b, Some(&topic.name));
            if version >= 1 {
                b.put_u8(if topic.is_internal { 1 } else { 0 });
            }

            write_array(b, Some(&topic.partitions), |bp, part| {
                bp.put_i16(part.error_code.as_i16());
                bp.put_i32(part.partition_index);
                bp.put_i32(part.leader_id);
                if version >= 7 {
                    bp.put_i32(part.leader_epoch.unwrap_or(-1));
                }

                write_array(bp, Some(&part.replica_nodes), |br, &r| br.put_i32(r));
                write_array(bp, Some(&part.isr_nodes), |bi, &i| bi.put_i32(i));
                if version >= 5 {
                    write_array(bp, Some(&part.offline_replicas), |bo, &o| bo.put_i32(o));
                }
            });
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_metadata_request_roundtrip() {
        let req = MetadataRequest::for_topics(vec!["events".to_string(), "orders".to_string()]);
        for v in 0..=7 {
            let mut buf = BytesMut::new();
            req.encode(v, &mut buf);

            let mut cursor = buf.freeze();
            let decoded = MetadataRequest::decode(v, &mut cursor).expect("Decode failed");
            assert_eq!(decoded.topics, req.topics);
        }
    }

    #[test]
    fn test_metadata_request_all_topics() {
        let req = MetadataRequest::all_topics();
        for v in 0..=7 {
            let mut buf = BytesMut::new();
            req.encode(v, &mut buf);

            let mut cursor = buf.freeze();
            let decoded = MetadataRequest::decode(v, &mut cursor).expect("Decode all failed");
            assert_eq!(decoded.topics, None);
        }
    }

    #[test]
    fn test_metadata_response_v0_through_v7_roundtrip() {
        let partitions = vec![
            PartitionMetadata::leader(0, 1),
            PartitionMetadata::leader(1, 1),
        ];
        let topic = TopicMetadata::success("payments", partitions);
        let resp = MetadataResponse::single_broker(1, "127.0.0.1", 9092, vec![topic]);

        for v in 0..=7 {
            let mut buf = BytesMut::new();
            resp.encode(v, &mut buf);

            let mut cursor = buf.freeze();
            let decoded = MetadataResponse::decode(v, &mut cursor)
                .unwrap_or_else(|e| panic!("Decode v{} failed: {}", v, e));

            assert_eq!(decoded.brokers.len(), 1);
            assert_eq!(decoded.brokers[0].node_id, 1);
            assert_eq!(decoded.brokers[0].host, "127.0.0.1");
            assert_eq!(decoded.brokers[0].port, 9092);

            assert_eq!(decoded.topics.len(), 1);
            assert_eq!(decoded.topics[0].name, "payments");
            assert_eq!(decoded.topics[0].partitions.len(), 2);
            assert_eq!(decoded.topics[0].partitions[0].partition_index, 0);
            assert_eq!(decoded.topics[0].partitions[0].leader_id, 1);
            assert_eq!(decoded.topics[0].partitions[1].partition_index, 1);

            if v >= 2 {
                assert_eq!(decoded.cluster_id, resp.cluster_id);
            }
            if v >= 1 {
                assert_eq!(decoded.controller_id, 1);
            }
            if v >= 7 {
                assert_eq!(decoded.topics[0].partitions[0].leader_epoch, Some(0));
            }
        }
    }
}
