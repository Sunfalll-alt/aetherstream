//! Binary log record format, serialization, and CRC32 verification.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32fast::Hasher;

use crate::error::StorageError;

/// Format version identifier for binary log records.
pub const CURRENT_RECORD_MAGIC: u8 = 1;

/// Default attribute flags (uncompressed, standard record).
pub const ATTR_NONE: u8 = 0;

/// Key-value metadata pair attached to a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// Header key name.
    pub key: String,
    /// Header value payload (optional).
    pub value: Option<Bytes>,
}

impl Header {
    /// Creates a new header with a key and value.
    pub fn new(key: impl Into<String>, value: impl Into<Option<Bytes>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// An individual commit log event record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Logical partition offset assigned to this record.
    pub offset: u64,
    /// Unix timestamp in milliseconds.
    pub timestamp: i64,
    /// Optional partitioning or entity key.
    pub key: Option<Bytes>,
    /// Optional event payload (None represents a tombstone deletion).
    pub value: Option<Bytes>,
    /// Associated metadata headers.
    pub headers: Vec<Header>,
    /// Record attributes (compression, transaction markers).
    pub attributes: u8,
}

impl Record {
    /// Creates a new record with current system timestamp and unassigned offset (0).
    pub fn new(key: impl Into<Option<Bytes>>, value: impl Into<Option<Bytes>>) -> Self {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        Self {
            offset: 0,
            timestamp: now_ms,
            key: key.into(),
            value: value.into(),
            headers: Vec::new(),
            attributes: ATTR_NONE,
        }
    }

    /// Attaches headers to the record in a builder pattern.
    pub fn with_headers(mut self, headers: Vec<Header>) -> Self {
        self.headers = headers;
        self
    }

    /// Overrides the timestamp on the record.
    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Overrides the offset on the record.
    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }

    /// Calculates the exact on-disk byte footprint of this record when serialized.
    pub fn encoded_size(&self) -> usize {
        // 4 (total_len) + 4 (crc32) + 1 (magic) + 1 (attributes) + 8 (offset) + 8 (timestamp)
        let mut size = 4 + 4 + 1 + 1 + 8 + 8;

        // Key length (4 bytes i32) + payload
        size += 4 + self.key.as_ref().map(|k| k.len()).unwrap_or(0);

        // Value length (4 bytes i32) + payload
        size += 4 + self.value.as_ref().map(|v| v.len()).unwrap_or(0);

        // Headers count (4 bytes u32)
        size += 4;
        for header in &self.headers {
            // Header key len (4 bytes u32) + key string bytes
            size += 4 + header.key.len();
            // Header val len (4 bytes i32) + val bytes
            size += 4 + header.value.as_ref().map(|v| v.len()).unwrap_or(0);
        }

        size
    }

    /// Serializes this record into a newly allocated zero-copy `Bytes` buffer.
    pub fn encode(&self) -> Bytes {
        let size = self.encoded_size();
        let mut buf = BytesMut::with_capacity(size);
        self.encode_into(&mut buf);
        buf.freeze()
    }

    /// Serializes this record into the provided `BytesMut` buffer.
    pub fn encode_into(&self, buf: &mut BytesMut) {
        let start_pos = buf.len();

        // Reserve space for total_len (4 bytes) and crc (4 bytes)
        buf.put_u32(0); // placeholder for total_len
        buf.put_u32(0); // placeholder for crc

        let body_start = buf.len();

        // Magic byte & attributes
        buf.put_u8(CURRENT_RECORD_MAGIC);
        buf.put_u8(self.attributes);

        // Offset & timestamp
        buf.put_u64(self.offset);
        buf.put_i64(self.timestamp);

        // Key (-1 indicates None)
        match &self.key {
            Some(key) => {
                buf.put_i32(key.len() as i32);
                buf.put_slice(key);
            }
            None => buf.put_i32(-1),
        }

        // Value (-1 indicates None)
        match &self.value {
            Some(val) => {
                buf.put_i32(val.len() as i32);
                buf.put_slice(val);
            }
            None => buf.put_i32(-1),
        }

        // Headers
        buf.put_u32(self.headers.len() as u32);
        for header in &self.headers {
            buf.put_u32(header.key.len() as u32);
            buf.put_slice(header.key.as_bytes());
            match &header.value {
                Some(val) => {
                    buf.put_i32(val.len() as i32);
                    buf.put_slice(val);
                }
                None => buf.put_i32(-1),
            }
        }

        let body_len = buf.len() - body_start;
        // Total framed length excluding the 4-byte total_len field itself = 4 (crc) + body_len
        let total_frame_len = (4 + body_len) as u32;

        // Compute CRC32 checksum over the record body (after CRC field)
        let mut hasher = Hasher::new();
        hasher.update(&buf[body_start..]);
        let calculated_crc = hasher.finalize();

        // Patch total_len and crc in-place
        let total_len_bytes = total_frame_len.to_be_bytes();
        let crc_bytes = calculated_crc.to_be_bytes();

        buf[start_pos..start_pos + 4].copy_from_slice(&total_len_bytes);
        buf[start_pos + 4..start_pos + 8].copy_from_slice(&crc_bytes);
    }

    /// Decodes a record from a raw byte slice, verifying integrity via CRC32.
    ///
    /// Returns the parsed `Record` and the total number of bytes consumed (including length header).
    pub fn decode_from_slice(data: &[u8]) -> Result<(Record, usize), StorageError> {
        if data.len() < 4 {
            return Err(StorageError::IncompleteRecord {
                expected: 4,
                available: data.len(),
            });
        }

        let total_frame_len = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
        let total_record_bytes = 4 + total_frame_len;

        if data.len() < total_record_bytes {
            return Err(StorageError::IncompleteRecord {
                expected: total_record_bytes,
                available: data.len(),
            });
        }

        if total_frame_len < 4 {
            return Err(StorageError::InvalidFormat(format!(
                "Frame length {} is too small for CRC32 field",
                total_frame_len
            )));
        }

        let frame = &data[4..total_record_bytes];
        let expected_crc = u32::from_be_bytes(frame[0..4].try_into().unwrap());
        let body = &frame[4..];

        // CRC32 verification
        let mut hasher = Hasher::new();
        hasher.update(body);
        let calculated_crc = hasher.finalize();

        if expected_crc != calculated_crc {
            return Err(StorageError::CrcMismatch {
                expected: expected_crc,
                calculated: calculated_crc,
            });
        }

        // Deserialize body
        let mut cursor = body;
        if cursor.len() < 1 + 1 + 8 + 8 + 4 + 4 + 4 {
            return Err(StorageError::InvalidFormat(
                "Record payload header is truncated".to_string(),
            ));
        }

        let magic = cursor.get_u8();
        if magic != CURRENT_RECORD_MAGIC {
            return Err(StorageError::InvalidFormat(format!(
                "Unsupported record magic version: {}",
                magic
            )));
        }

        let attributes = cursor.get_u8();
        let offset = cursor.get_u64();
        let timestamp = cursor.get_i64();

        // Key
        let key_len = cursor.get_i32();
        let key = if key_len >= 0 {
            let klen = key_len as usize;
            if cursor.remaining() < klen {
                return Err(StorageError::InvalidFormat("Truncated key payload".into()));
            }
            Some(cursor.copy_to_bytes(klen))
        } else {
            None
        };

        // Value
        let value_len = cursor.get_i32();
        let value = if value_len >= 0 {
            let vlen = value_len as usize;
            if cursor.remaining() < vlen {
                return Err(StorageError::InvalidFormat(
                    "Truncated value payload".into(),
                ));
            }
            Some(cursor.copy_to_bytes(vlen))
        } else {
            None
        };

        // Headers
        if cursor.remaining() < 4 {
            return Err(StorageError::InvalidFormat("Truncated header count".into()));
        }
        let headers_count = cursor.get_u32() as usize;
        let mut headers = Vec::with_capacity(headers_count);

        for _ in 0..headers_count {
            if cursor.remaining() < 4 {
                return Err(StorageError::InvalidFormat(
                    "Truncated header key length".into(),
                ));
            }
            let hkey_len = cursor.get_u32() as usize;
            if cursor.remaining() < hkey_len {
                return Err(StorageError::InvalidFormat("Truncated header key".into()));
            }
            let hkey_bytes = cursor.copy_to_bytes(hkey_len);
            let hkey = String::from_utf8(hkey_bytes.to_vec()).map_err(|e| {
                StorageError::InvalidFormat(format!("Invalid UTF-8 header key: {}", e))
            })?;

            if cursor.remaining() < 4 {
                return Err(StorageError::InvalidFormat(
                    "Truncated header value length".into(),
                ));
            }
            let hval_len = cursor.get_i32();
            let hval = if hval_len >= 0 {
                let vlen = hval_len as usize;
                if cursor.remaining() < vlen {
                    return Err(StorageError::InvalidFormat("Truncated header value".into()));
                }
                Some(cursor.copy_to_bytes(vlen))
            } else {
                None
            };

            headers.push(Header {
                key: hkey,
                value: hval,
            });
        }

        Ok((
            Record {
                offset,
                timestamp,
                key,
                value,
                headers,
                attributes,
            },
            total_record_bytes,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_roundtrip_simple() {
        let record = Record::new(
            Some(Bytes::from_static(b"order-key-101")),
            Some(Bytes::from_static(b"order-payload-{\"amount\":42}")),
        )
        .with_offset(1005)
        .with_timestamp(1728000000000);

        let encoded = record.encode();
        assert_eq!(encoded.len(), record.encoded_size());

        let (decoded, consumed) =
            Record::decode_from_slice(&encoded).expect("Decode should succeed");
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.offset, 1005);
        assert_eq!(decoded.timestamp, 1728000000000);
        assert_eq!(decoded.key, Some(Bytes::from_static(b"order-key-101")));
        assert_eq!(
            decoded.value,
            Some(Bytes::from_static(b"order-payload-{\"amount\":42}"))
        );
        assert!(decoded.headers.is_empty());
    }

    #[test]
    fn test_record_roundtrip_with_headers_and_nulls() {
        let headers = vec![
            Header::new("trace-id", Some(Bytes::from_static(b"abc-xyz-123"))),
            Header::new("source", Some(Bytes::from_static(b"gateway-eu-1"))),
            Header::new("empty-marker", None),
        ];

        let record = Record::new(None, None)
            .with_offset(0)
            .with_headers(headers.clone());

        let encoded = record.encode();
        let (decoded, consumed) =
            Record::decode_from_slice(&encoded).expect("Decode should succeed");
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.offset, 0);
        assert_eq!(decoded.key, None);
        assert_eq!(decoded.value, None);
        assert_eq!(decoded.headers.len(), 3);
        assert_eq!(decoded.headers, headers);
    }

    #[test]
    fn test_record_crc_corruption_detection() {
        let record = Record::new(
            Some(Bytes::from_static(b"key")),
            Some(Bytes::from_static(b"value")),
        )
        .with_offset(42);

        let mut encoded = record.encode().to_vec();

        // Mutate a byte in the payload body (offset 15)
        let last_idx = encoded.len() - 1;
        encoded[last_idx] ^= 0xFF;

        let result = Record::decode_from_slice(&encoded);
        match result {
            Err(StorageError::CrcMismatch {
                expected,
                calculated,
            }) => {
                assert_ne!(expected, calculated);
            }
            other => panic!("Expected CrcMismatch, got {:?}", other),
        }
    }

    #[test]
    fn test_record_incomplete_frame() {
        let record = Record::new(
            Some(Bytes::from_static(b"k")),
            Some(Bytes::from_static(b"v")),
        );
        let encoded = record.encode();

        // Truncate encoded bytes
        let truncated = &encoded[0..encoded.len() - 5];
        let result = Record::decode_from_slice(truncated);
        match result {
            Err(StorageError::IncompleteRecord {
                expected,
                available,
            }) => {
                assert_eq!(expected, encoded.len());
                assert_eq!(available, truncated.len());
            }
            other => panic!("Expected IncompleteRecord, got {:?}", other),
        }
    }
}
