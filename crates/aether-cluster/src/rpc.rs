//! Raft inter-broker consensus and replication RPC codecs.

use aether_protocol::ProtocolError;
use aether_storage::Record;
use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

/// Message type discriminator tags.
pub const MSG_VOTE_REQUEST: u8 = 1;
pub const MSG_VOTE_RESPONSE: u8 = 2;
pub const MSG_APPEND_ENTRIES_REQUEST: u8 = 3;
pub const MSG_APPEND_ENTRIES_RESPONSE: u8 = 4;

/// VoteRequest RPC sent by candidates during leader election.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteRequest {
    /// Candidate's election term.
    pub term: u64,
    /// Candidate broker ID.
    pub candidate_id: i32,
    /// Term of candidate's last log entry.
    pub last_log_term: u64,
    /// Offset of candidate's last log entry.
    pub last_log_offset: u64,
}

impl VoteRequest {
    /// Encodes into a `BytesMut` buffer.
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.term);
        buf.put_i32(self.candidate_id);
        buf.put_u64(self.last_log_term);
        buf.put_u64(self.last_log_offset);
    }

    /// Decodes from an incoming buffer.
    pub fn decode(buf: &mut impl Buf) -> Result<Self, ProtocolError> {
        if buf.remaining() < 28 {
            return Err(ProtocolError::UnexpectedEof {
                expected: 28,
                available: buf.remaining(),
            });
        }
        Ok(Self {
            term: buf.get_u64(),
            candidate_id: buf.get_i32(),
            last_log_term: buf.get_u64(),
            last_log_offset: buf.get_u64(),
        })
    }
}

/// VoteResponse RPC returned by replicas to candidates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteResponse {
    /// Current term of the voter.
    pub term: u64,
    /// True if the voter granted its vote to the candidate.
    pub vote_granted: bool,
}

impl VoteResponse {
    /// Encodes into a `BytesMut` buffer.
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.term);
        buf.put_u8(if self.vote_granted { 1 } else { 0 });
    }

    /// Decodes from an incoming buffer.
    pub fn decode(buf: &mut impl Buf) -> Result<Self, ProtocolError> {
        if buf.remaining() < 9 {
            return Err(ProtocolError::UnexpectedEof {
                expected: 9,
                available: buf.remaining(),
            });
        }
        Ok(Self {
            term: buf.get_u64(),
            vote_granted: buf.get_u8() != 0,
        })
    }
}

/// AppendEntriesRequest RPC sent by leaders to replicate log batches and heartbeats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesRequest {
    /// Leader's current term.
    pub term: u64,
    /// Leader broker node ID.
    pub leader_id: i32,
    /// Target topic name.
    pub topic: String,
    /// Target partition index.
    pub partition_id: u32,
    /// Term of log entry immediately preceding the new entries.
    pub prev_log_term: u64,
    /// Offset of log entry immediately preceding the new entries.
    pub prev_log_offset: u64,
    /// Leader's committed offset (High Watermark).
    pub leader_commit: u64,
    /// Log records to replicate (empty for heartbeats).
    pub entries: Vec<Record>,
}

impl AppendEntriesRequest {
    /// Encodes into a `BytesMut` buffer.
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.term);
        buf.put_i32(self.leader_id);

        let topic_bytes = self.topic.as_bytes();
        buf.put_u16(topic_bytes.len() as u16);
        buf.put_slice(topic_bytes);

        buf.put_u32(self.partition_id);
        buf.put_u64(self.prev_log_term);
        buf.put_u64(self.prev_log_offset);
        buf.put_u64(self.leader_commit);

        buf.put_u32(self.entries.len() as u32);
        for entry in &self.entries {
            entry.encode_into(buf);
        }
    }

    /// Decodes from an incoming buffer.
    pub fn decode(buf: &mut impl Buf) -> Result<Self, ProtocolError> {
        if buf.remaining() < 14 {
            return Err(ProtocolError::UnexpectedEof {
                expected: 14,
                available: buf.remaining(),
            });
        }
        let term = buf.get_u64();
        let leader_id = buf.get_i32();

        let topic_len = buf.get_u16() as usize;
        if buf.remaining() < topic_len + 4 + 8 + 8 + 8 + 4 {
            return Err(ProtocolError::UnexpectedEof {
                expected: topic_len + 32,
                available: buf.remaining(),
            });
        }
        let mut topic_bytes = vec![0u8; topic_len];
        buf.copy_to_slice(&mut topic_bytes);
        let topic = String::from_utf8(topic_bytes)?;

        let partition_id = buf.get_u32();
        let prev_log_term = buf.get_u64();
        let prev_log_offset = buf.get_u64();
        let leader_commit = buf.get_u64();

        let entries_count = buf.get_u32() as usize;
        let mut entries = Vec::with_capacity(entries_count);

        let remaining_slice = buf.chunk();
        let mut cursor: &[u8] = remaining_slice;
        let mut total_consumed = 0;

        for _ in 0..entries_count {
            let (rec, consumed) = aether_storage::Record::decode_from_slice(cursor)
                .map_err(|e| ProtocolError::InvalidProtocol(e.to_string()))?;
            entries.push(rec);
            cursor = &cursor[consumed..];
            total_consumed += consumed;
        }
        buf.advance(total_consumed);

        Ok(Self {
            term,
            leader_id,
            topic,
            partition_id,
            prev_log_term,
            prev_log_offset,
            leader_commit,
            entries,
        })
    }
}

/// AppendEntriesResponse RPC returned by followers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    /// Follower's current term.
    pub term: u64,
    /// Follower broker node ID.
    pub follower_id: i32,
    /// Target topic name.
    pub topic: String,
    /// Target partition index.
    pub partition_id: u32,
    /// True if follower contained entry matching `prev_log_offset`.
    pub success: bool,
    /// Match offset: current Log End Offset on follower.
    pub match_offset: u64,
}

impl AppendEntriesResponse {
    /// Encodes into a `BytesMut` buffer.
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.term);
        buf.put_i32(self.follower_id);

        let topic_bytes = self.topic.as_bytes();
        buf.put_u16(topic_bytes.len() as u16);
        buf.put_slice(topic_bytes);

        buf.put_u32(self.partition_id);
        buf.put_u8(if self.success { 1 } else { 0 });
        buf.put_u64(self.match_offset);
    }

    /// Decodes from an incoming buffer.
    pub fn decode(buf: &mut impl Buf) -> Result<Self, ProtocolError> {
        if buf.remaining() < 14 {
            return Err(ProtocolError::UnexpectedEof {
                expected: 14,
                available: buf.remaining(),
            });
        }
        let term = buf.get_u64();
        let follower_id = buf.get_i32();

        let topic_len = buf.get_u16() as usize;
        if buf.remaining() < topic_len + 4 + 1 + 8 {
            return Err(ProtocolError::UnexpectedEof {
                expected: topic_len + 13,
                available: buf.remaining(),
            });
        }
        let mut topic_bytes = vec![0u8; topic_len];
        buf.copy_to_slice(&mut topic_bytes);
        let topic = String::from_utf8(topic_bytes)?;

        let partition_id = buf.get_u32();
        let success = buf.get_u8() != 0;
        let match_offset = buf.get_u64();

        Ok(Self {
            term,
            follower_id,
            topic,
            partition_id,
            success,
            match_offset,
        })
    }
}

/// Unified inter-broker cluster message envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterMessage {
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
}

/// Framing codec for cluster RPC connections over TCP.
#[derive(Debug, Default, Clone)]
pub struct ClusterCodec {
    max_frame_size: usize,
}

impl ClusterCodec {
    /// Creates a codec with default 64 MiB maximum frame size.
    pub fn new() -> Self {
        Self {
            max_frame_size: 64 * 1024 * 1024,
        }
    }
}

impl Decoder for ClusterCodec {
    type Item = ClusterMessage;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let frame_len = u32::from_be_bytes(length_bytes) as usize;

        if frame_len > self.max_frame_size {
            return Err(ProtocolError::FrameTooLarge {
                size: frame_len,
                limit: self.max_frame_size,
            });
        }

        if src.len() < 4 + frame_len {
            src.reserve(4 + frame_len - src.len());
            return Ok(None);
        }

        src.advance(4);
        let mut frame_buf = src.split_to(frame_len);

        if frame_buf.remaining() < 1 {
            return Err(ProtocolError::UnexpectedEof {
                expected: 1,
                available: 0,
            });
        }

        let msg_type = frame_buf.get_u8();
        let msg = match msg_type {
            MSG_VOTE_REQUEST => ClusterMessage::VoteRequest(VoteRequest::decode(&mut frame_buf)?),
            MSG_VOTE_RESPONSE => {
                ClusterMessage::VoteResponse(VoteResponse::decode(&mut frame_buf)?)
            }
            MSG_APPEND_ENTRIES_REQUEST => {
                ClusterMessage::AppendEntriesRequest(AppendEntriesRequest::decode(&mut frame_buf)?)
            }
            MSG_APPEND_ENTRIES_RESPONSE => ClusterMessage::AppendEntriesResponse(
                AppendEntriesResponse::decode(&mut frame_buf)?,
            ),
            unknown => return Err(ProtocolError::UnsupportedApiKey(unknown as i16)),
        };

        Ok(Some(msg))
    }
}

impl Encoder<ClusterMessage> for ClusterCodec {
    type Error = ProtocolError;

    fn encode(&mut self, item: ClusterMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let length_offset = dst.len();
        dst.put_u32(0); // placeholder for frame length
        let body_start = dst.len();

        match item {
            ClusterMessage::VoteRequest(req) => {
                dst.put_u8(MSG_VOTE_REQUEST);
                req.encode(dst);
            }
            ClusterMessage::VoteResponse(resp) => {
                dst.put_u8(MSG_VOTE_RESPONSE);
                resp.encode(dst);
            }
            ClusterMessage::AppendEntriesRequest(req) => {
                dst.put_u8(MSG_APPEND_ENTRIES_REQUEST);
                req.encode(dst);
            }
            ClusterMessage::AppendEntriesResponse(resp) => {
                dst.put_u8(MSG_APPEND_ENTRIES_RESPONSE);
                resp.encode(dst);
            }
        }

        let frame_len = dst.len() - body_start;
        let len_bytes = (frame_len as u32).to_be_bytes();
        dst[length_offset..length_offset + 4].copy_from_slice(&len_bytes);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_vote_request_response_roundtrip() {
        let req = VoteRequest {
            term: 42,
            candidate_id: 2,
            last_log_term: 41,
            last_log_offset: 100500,
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        let decoded = VoteRequest::decode(&mut buf).unwrap();
        assert_eq!(req, decoded);

        let resp = VoteResponse {
            term: 42,
            vote_granted: true,
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf);
        let decoded_resp = VoteResponse::decode(&mut buf).unwrap();
        assert_eq!(resp, decoded_resp);
    }

    #[test]
    fn test_append_entries_roundtrip() {
        let rec = Record::new(
            Some(Bytes::from_static(b"k1")),
            Some(Bytes::from_static(b"v1")),
        );
        let req = AppendEntriesRequest {
            term: 5,
            leader_id: 1,
            topic: "test-topic".to_string(),
            partition_id: 0,
            prev_log_term: 4,
            prev_log_offset: 10,
            leader_commit: 10,
            entries: vec![rec],
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        let decoded = AppendEntriesRequest::decode(&mut buf).unwrap();
        assert_eq!(req, decoded);
    }
}
