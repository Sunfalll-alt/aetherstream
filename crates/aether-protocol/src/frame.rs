//! 4-byte length-prefixed frame decoder and encoder for the Kafka wire protocol.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::error::ProtocolError;
use crate::header::RequestHeader;

/// Default maximum allowable Kafka frame size (100 MiB).
pub const DEFAULT_MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

/// Low-level frame codec implementing 4-byte big-endian framing over streaming TCP connections.
#[derive(Debug, Clone)]
pub struct KafkaCodec {
    max_frame_bytes: usize,
}

impl Default for KafkaCodec {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        }
    }
}

impl KafkaCodec {
    /// Creates a new `KafkaCodec` with a specified maximum frame size.
    pub fn new(max_frame_bytes: usize) -> Self {
        Self { max_frame_bytes }
    }
}

impl Decoder for KafkaCodec {
    type Item = Bytes;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least 4 bytes to determine total frame length
        if src.len() < 4 {
            return Ok(None);
        }

        let length = i32::from_be_bytes(src[0..4].try_into().unwrap());

        if length <= 0 {
            return Err(ProtocolError::InvalidFrameSize(length));
        }

        let frame_len = length as usize;
        if frame_len > self.max_frame_bytes {
            return Err(ProtocolError::FrameTooLarge {
                size: frame_len,
                limit: self.max_frame_bytes,
            });
        }

        let total_bytes = 4 + frame_len;
        if src.len() < total_bytes {
            // Pre-reserve capacity to avoid re-allocations as TCP stream arrives
            src.reserve(total_bytes - src.len());
            return Ok(None);
        }

        // Consume 4-byte length header
        src.advance(4);

        // Split off the frame payload
        let frame = src.split_to(frame_len).freeze();
        Ok(Some(frame))
    }
}

impl Encoder<Bytes> for KafkaCodec {
    type Error = ProtocolError;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if item.len() > self.max_frame_bytes {
            return Err(ProtocolError::FrameTooLarge {
                size: item.len(),
                limit: self.max_frame_bytes,
            });
        }

        dst.reserve(4 + item.len());
        dst.put_i32(item.len() as i32);
        dst.put_slice(&item);
        Ok(())
    }
}

/// An incoming framed Kafka request containing parsed `RequestHeader` and remaining payload `body`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundRequest {
    /// The decoded request header.
    pub header: RequestHeader,
    /// Unconsumed request body payload.
    pub body: Bytes,
}

impl InboundRequest {
    /// Parses an incoming raw frame into an `InboundRequest`.
    pub fn from_frame(mut frame: Bytes) -> Result<Self, ProtocolError> {
        let header = RequestHeader::decode(&mut frame)?;
        Ok(Self {
            header,
            body: frame,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_codec_frame_roundtrip() {
        let mut codec = KafkaCodec::default();
        let payload = Bytes::from_static(b"kafka-binary-payload-data");

        let mut encoded = BytesMut::new();
        codec.encode(payload.clone(), &mut encoded).unwrap();
        assert_eq!(encoded.len(), 4 + payload.len());

        let decoded = codec.decode(&mut encoded).unwrap().unwrap();
        assert_eq!(decoded, payload);
        assert_eq!(encoded.len(), 0);
    }

    #[test]
    fn test_codec_partial_frame_streaming() {
        let mut codec = KafkaCodec::default();
        let payload = Bytes::from_static(b"long-payload-message-test");

        let mut encoded = BytesMut::new();
        codec.encode(payload.clone(), &mut encoded).unwrap();

        // Feed only first 10 bytes
        let mut partial = encoded.split_to(10);
        assert_eq!(codec.decode(&mut partial).unwrap(), None);

        // Append remaining bytes
        partial.unsplit(encoded);
        let decoded = codec.decode(&mut partial).unwrap().unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_codec_frame_too_large() {
        let mut codec = KafkaCodec::new(100);
        let large_payload = Bytes::from(vec![0u8; 101]);

        let mut encoded = BytesMut::new();
        let err = codec.encode(large_payload, &mut encoded).unwrap_err();
        match err {
            ProtocolError::FrameTooLarge { size, limit } => {
                assert_eq!(size, 101);
                assert_eq!(limit, 100);
            }
            other => panic!("Expected FrameTooLarge, got {:?}", other),
        }
    }
}
