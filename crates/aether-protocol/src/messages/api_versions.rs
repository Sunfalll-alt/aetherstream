//! ApiVersions API (v0 - v3) request parser and response generator.

use bytes::{Buf, BufMut};

use crate::error::ProtocolError;
use crate::header::{ApiKey, ErrorCode};
use crate::primitives::{
    read_array, read_compact_array, read_compact_string, skip_tagged_fields, write_array,
    write_compact_array, write_compact_string, write_empty_tagged_fields,
};

/// Advertised supported API key and version range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiVersionSupport {
    /// The API key identifier.
    pub api_key: i16,
    /// Minimum version supported by the broker.
    pub min_version: i16,
    /// Maximum version supported by the broker.
    pub max_version: i16,
}

impl ApiVersionSupport {
    /// Creates a new `ApiVersionSupport` descriptor.
    pub const fn new(api_key: ApiKey, min_version: i16, max_version: i16) -> Self {
        Self {
            api_key: api_key.as_i16(),
            min_version,
            max_version,
        }
    }
}

/// ApiVersionsRequest (v0 - v3).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ApiVersionsRequest {
    /// Client software name (v3+).
    pub client_software_name: Option<String>,
    /// Client software version (v3+).
    pub client_software_version: Option<String>,
}

impl ApiVersionsRequest {
    /// Decodes an ApiVersionsRequest from the incoming buffer.
    pub fn decode(version: i16, buf: &mut impl Buf) -> Result<Self, ProtocolError> {
        if version >= 3 && buf.has_remaining() {
            let client_software_name = read_compact_string(buf)?;
            let client_software_version = read_compact_string(buf)?;
            skip_tagged_fields(buf)?;
            Ok(Self {
                client_software_name,
                client_software_version,
            })
        } else {
            // v0 - v2 request body is empty
            Ok(Self::default())
        }
    }

    /// Encodes an ApiVersionsRequest into the buffer.
    pub fn encode(&self, version: i16, buf: &mut impl BufMut) {
        if version >= 3 {
            write_compact_string(buf, self.client_software_name.as_deref());
            write_compact_string(buf, self.client_software_version.as_deref());
            write_empty_tagged_fields(buf);
        }
    }
}

/// ApiVersionsResponse (v0 - v3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsResponse {
    /// Top-level error code.
    pub error_code: ErrorCode,
    /// List of supported API keys and their versions.
    pub api_keys: Vec<ApiVersionSupport>,
    /// Throttle time in milliseconds (v1+).
    pub throttle_time_ms: i32,
}

impl ApiVersionsResponse {
    /// Generates the default response advertising AetherStream's supported Kafka APIs.
    pub fn default_supported() -> Self {
        Self {
            error_code: ErrorCode::None,
            api_keys: vec![
                ApiVersionSupport::new(ApiKey::Produce, 0, 8),
                ApiVersionSupport::new(ApiKey::Fetch, 0, 11),
                ApiVersionSupport::new(ApiKey::Metadata, 0, 7),
                ApiVersionSupport::new(ApiKey::ApiVersions, 0, 3),
            ],
            throttle_time_ms: 0,
        }
    }

    /// Creates an error response when client requests an unsupported protocol version.
    pub fn unsupported_version() -> Self {
        Self {
            error_code: ErrorCode::UnsupportedVersion,
            api_keys: Vec::new(),
            throttle_time_ms: 0,
        }
    }

    /// Decodes an ApiVersionsResponse from the buffer according to the negotiated version.
    pub fn decode(version: i16, buf: &mut impl Buf) -> Result<Self, ProtocolError> {
        if buf.remaining() < 2 {
            return Err(ProtocolError::UnexpectedEof {
                expected: 2,
                available: buf.remaining(),
            });
        }

        let error_code = ErrorCode::from_i16(buf.get_i16());

        let api_keys = if version >= 3 {
            // Flexible compact array (v3)
            read_compact_array(buf, |b| {
                if b.remaining() < 6 {
                    return Err(ProtocolError::UnexpectedEof {
                        expected: 6,
                        available: b.remaining(),
                    });
                }
                let api_key = b.get_i16();
                let min_version = b.get_i16();
                let max_version = b.get_i16();
                skip_tagged_fields(b)?;
                Ok(ApiVersionSupport {
                    api_key,
                    min_version,
                    max_version,
                })
            })?
            .unwrap_or_default()
        } else {
            // Standard array (v0 - v2)
            read_array(buf, |b| {
                if b.remaining() < 6 {
                    return Err(ProtocolError::UnexpectedEof {
                        expected: 6,
                        available: b.remaining(),
                    });
                }
                let api_key = b.get_i16();
                let min_version = b.get_i16();
                let max_version = b.get_i16();
                Ok(ApiVersionSupport {
                    api_key,
                    min_version,
                    max_version,
                })
            })?
            .unwrap_or_default()
        };

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

        if version >= 3 && buf.has_remaining() {
            skip_tagged_fields(buf)?;
        }

        Ok(Self {
            error_code,
            api_keys,
            throttle_time_ms,
        })
    }

    /// Encodes this ApiVersionsResponse into the buffer according to `version`.
    pub fn encode(&self, version: i16, buf: &mut impl BufMut) {
        buf.put_i16(self.error_code.as_i16());

        if version >= 3 {
            // Compact array (v3)
            write_compact_array(buf, Some(&self.api_keys), |b, k| {
                b.put_i16(k.api_key);
                b.put_i16(k.min_version);
                b.put_i16(k.max_version);
                write_empty_tagged_fields(b);
            });
            buf.put_i32(self.throttle_time_ms);
            write_empty_tagged_fields(buf);
        } else {
            // Standard array (v0 - v2)
            write_array(buf, Some(&self.api_keys), |b, k| {
                b.put_i16(k.api_key);
                b.put_i16(k.min_version);
                b.put_i16(k.max_version);
            });
            if version >= 1 {
                buf.put_i32(self.throttle_time_ms);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_api_versions_response_v0_roundtrip() {
        let resp = ApiVersionsResponse::default_supported();
        let mut buf = BytesMut::new();
        resp.encode(0, &mut buf);

        let mut cursor = buf.freeze();
        let decoded = ApiVersionsResponse::decode(0, &mut cursor).expect("Decode v0 failed");
        assert_eq!(decoded.error_code, ErrorCode::None);
        assert_eq!(decoded.api_keys.len(), 4);
        assert_eq!(decoded.api_keys, resp.api_keys);
        assert_eq!(decoded.throttle_time_ms, 0);
        assert_eq!(cursor.remaining(), 0);
    }

    #[test]
    fn test_api_versions_response_v1_v2_roundtrip() {
        let mut resp = ApiVersionsResponse::default_supported();
        resp.throttle_time_ms = 42;

        for v in [1, 2] {
            let mut buf = BytesMut::new();
            resp.encode(v, &mut buf);

            let mut cursor = buf.freeze();
            let decoded = ApiVersionsResponse::decode(v, &mut cursor).expect("Decode v1/v2 failed");
            assert_eq!(decoded.error_code, ErrorCode::None);
            assert_eq!(decoded.api_keys, resp.api_keys);
            assert_eq!(decoded.throttle_time_ms, 42);
            assert_eq!(cursor.remaining(), 0);
        }
    }

    #[test]
    fn test_api_versions_response_v3_compact_roundtrip() {
        let mut resp = ApiVersionsResponse::default_supported();
        resp.throttle_time_ms = 100;

        let mut buf = BytesMut::new();
        resp.encode(3, &mut buf);

        let mut cursor = buf.freeze();
        let decoded = ApiVersionsResponse::decode(3, &mut cursor).expect("Decode v3 failed");
        assert_eq!(decoded.error_code, ErrorCode::None);
        assert_eq!(decoded.api_keys, resp.api_keys);
        assert_eq!(decoded.throttle_time_ms, 100);
        assert_eq!(cursor.remaining(), 0);
    }

    #[test]
    fn test_api_versions_request_v3_roundtrip() {
        let req = ApiVersionsRequest {
            client_software_name: Some("kcat".to_string()),
            client_software_version: Some("1.7.0".to_string()),
        };

        let mut buf = BytesMut::new();
        req.encode(3, &mut buf);

        let mut cursor = buf.freeze();
        let decoded = ApiVersionsRequest::decode(3, &mut cursor).unwrap();
        assert_eq!(decoded, req);
    }
}
