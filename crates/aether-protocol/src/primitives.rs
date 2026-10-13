//! Kafka protocol primitive type codecs (strings, byte arrays, varints, arrays).

use bytes::{Buf, BufMut, Bytes};

use crate::error::ProtocolError;

// -----------------------------------------------------------------------------
// Varints & Zigzag Encoding
// -----------------------------------------------------------------------------

/// Writes an unsigned 32-bit varint to the buffer.
pub fn write_uvarint(buf: &mut impl BufMut, mut value: u32) {
    while value >= 0x80 {
        buf.put_u8(((value as u8) & 0x7F) | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

/// Reads an unsigned 32-bit varint from the buffer.
pub fn read_uvarint(buf: &mut impl Buf) -> Result<u32, ProtocolError> {
    let mut result: u32 = 0;
    let mut shift: u32 = 0;

    loop {
        if !buf.has_remaining() {
            return Err(ProtocolError::UnexpectedEof {
                expected: 1,
                available: 0,
            });
        }
        let byte = buf.get_u8();
        result |= ((byte & 0x7F) as u32) << shift;
        if (byte & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift > 35 {
            return Err(ProtocolError::InvalidProtocol("Varint overflow".into()));
        }
    }
}

/// Writes a signed 32-bit integer as a zigzag-encoded varint.
pub fn write_varint(buf: &mut impl BufMut, value: i32) {
    let zigzag = ((value << 1) ^ (value >> 31)) as u32;
    write_uvarint(buf, zigzag);
}

/// Reads a signed 32-bit integer as a zigzag-encoded varint.
pub fn read_varint(buf: &mut impl Buf) -> Result<i32, ProtocolError> {
    let raw = read_uvarint(buf)?;
    let decoded = ((raw >> 1) as i32) ^ (-((raw & 1) as i32));
    Ok(decoded)
}

// -----------------------------------------------------------------------------
// Strings (Standard: INT16 length, Compact: UVARINT length + 1)
// -----------------------------------------------------------------------------

/// Reads a standard Kafka `STRING` (INT16 length prefix, -1 indicates null).
pub fn read_string(buf: &mut impl Buf) -> Result<Option<String>, ProtocolError> {
    if buf.remaining() < 2 {
        return Err(ProtocolError::UnexpectedEof {
            expected: 2,
            available: buf.remaining(),
        });
    }

    let len = buf.get_i16();
    if len == -1 {
        return Ok(None);
    }
    if len < -1 {
        return Err(ProtocolError::InvalidStringLength(len as i32));
    }

    let ulen = len as usize;
    if buf.remaining() < ulen {
        return Err(ProtocolError::UnexpectedEof {
            expected: ulen,
            available: buf.remaining(),
        });
    }

    let mut bytes = vec![0u8; ulen];
    buf.copy_to_slice(&mut bytes);
    let s = String::from_utf8(bytes)?;
    Ok(Some(s))
}

/// Writes a standard Kafka `STRING` (INT16 length prefix, -1 indicates null).
pub fn write_string(buf: &mut impl BufMut, s: Option<&str>) {
    match s {
        Some(s) => {
            buf.put_i16(s.len() as i16);
            buf.put_slice(s.as_bytes());
        }
        None => buf.put_i16(-1),
    }
}

/// Reads a Kafka `COMPACT_STRING` (UVARINT length + 1 prefix, 0 indicates null).
pub fn read_compact_string(buf: &mut impl Buf) -> Result<Option<String>, ProtocolError> {
    let ulen = read_uvarint(buf)?;
    if ulen == 0 {
        return Ok(None);
    }

    let len = (ulen - 1) as usize;
    if buf.remaining() < len {
        return Err(ProtocolError::UnexpectedEof {
            expected: len,
            available: buf.remaining(),
        });
    }

    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    let s = String::from_utf8(bytes)?;
    Ok(Some(s))
}

/// Writes a Kafka `COMPACT_STRING` (UVARINT length + 1 prefix, 0 indicates null).
pub fn write_compact_string(buf: &mut impl BufMut, s: Option<&str>) {
    match s {
        Some(s) => {
            write_uvarint(buf, (s.len() + 1) as u32);
            buf.put_slice(s.as_bytes());
        }
        None => write_uvarint(buf, 0),
    }
}

// -----------------------------------------------------------------------------
// Bytes (Standard: INT32 length, Compact: UVARINT length + 1)
// -----------------------------------------------------------------------------

/// Reads a standard Kafka `BYTES` (INT32 length prefix, -1 indicates null).
pub fn read_bytes(buf: &mut impl Buf) -> Result<Option<Bytes>, ProtocolError> {
    if buf.remaining() < 4 {
        return Err(ProtocolError::UnexpectedEof {
            expected: 4,
            available: buf.remaining(),
        });
    }

    let len = buf.get_i32();
    if len == -1 {
        return Ok(None);
    }
    if len < -1 {
        return Err(ProtocolError::InvalidProtocol(format!(
            "Invalid byte length {}",
            len
        )));
    }

    let ulen = len as usize;
    if buf.remaining() < ulen {
        return Err(ProtocolError::UnexpectedEof {
            expected: ulen,
            available: buf.remaining(),
        });
    }

    let slice = buf.copy_to_bytes(ulen);
    Ok(Some(slice))
}

/// Writes a standard Kafka `BYTES` (INT32 length prefix, -1 indicates null).
pub fn write_bytes(buf: &mut impl BufMut, b: Option<&[u8]>) {
    match b {
        Some(b) => {
            buf.put_i32(b.len() as i32);
            buf.put_slice(b);
        }
        None => buf.put_i32(-1),
    }
}

// -----------------------------------------------------------------------------
// Arrays (Standard: INT32 length, Compact: UVARINT length + 1)
// -----------------------------------------------------------------------------

/// Reads a standard Kafka array with an INT32 length prefix (-1 indicates null).
pub fn read_array<B, T, F>(buf: &mut B, mut read_item: F) -> Result<Option<Vec<T>>, ProtocolError>
where
    B: Buf,
    F: FnMut(&mut B) -> Result<T, ProtocolError>,
{
    if buf.remaining() < 4 {
        return Err(ProtocolError::UnexpectedEof {
            expected: 4,
            available: buf.remaining(),
        });
    }

    let len = buf.get_i32();
    if len == -1 {
        return Ok(None);
    }
    if len < -1 {
        return Err(ProtocolError::InvalidProtocol(format!(
            "Invalid array count: {}",
            len
        )));
    }

    let count = len as usize;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(read_item(buf)?);
    }
    Ok(Some(items))
}

/// Writes a standard Kafka array with an INT32 length prefix (-1 indicates null).
pub fn write_array<B, T, F>(buf: &mut B, items: Option<&[T]>, mut write_item: F)
where
    B: BufMut,
    F: FnMut(&mut B, &T),
{
    match items {
        Some(slice) => {
            buf.put_i32(slice.len() as i32);
            for item in slice {
                write_item(buf, item);
            }
        }
        None => buf.put_i32(-1),
    }
}

/// Reads a compact Kafka array with a UVARINT length + 1 prefix (0 indicates null).
pub fn read_compact_array<B, T, F>(
    buf: &mut B,
    mut read_item: F,
) -> Result<Option<Vec<T>>, ProtocolError>
where
    B: Buf,
    F: FnMut(&mut B) -> Result<T, ProtocolError>,
{
    let ulen = read_uvarint(buf)?;
    if ulen == 0 {
        return Ok(None);
    }

    let count = (ulen - 1) as usize;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(read_item(buf)?);
    }
    Ok(Some(items))
}

/// Writes a compact Kafka array with a UVARINT length + 1 prefix (0 indicates null).
pub fn write_compact_array<B, T, F>(buf: &mut B, items: Option<&[T]>, mut write_item: F)
where
    B: BufMut,
    F: FnMut(&mut B, &T),
{
    match items {
        Some(slice) => {
            write_uvarint(buf, (slice.len() + 1) as u32);
            for item in slice {
                write_item(buf, item);
            }
        }
        None => write_uvarint(buf, 0),
    }
}

// -----------------------------------------------------------------------------
// Tagged Fields (KIP-482 Flexible Versions)
// -----------------------------------------------------------------------------

/// Skips any trailing tagged fields in flexible protocol versions.
pub fn skip_tagged_fields(buf: &mut impl Buf) -> Result<(), ProtocolError> {
    let num_tagged_fields = read_uvarint(buf)?;
    for _ in 0..num_tagged_fields {
        let _tag = read_uvarint(buf)?;
        let data_len = read_uvarint(buf)? as usize;
        if buf.remaining() < data_len {
            return Err(ProtocolError::UnexpectedEof {
                expected: data_len,
                available: buf.remaining(),
            });
        }
        buf.advance(data_len);
    }
    Ok(())
}

/// Writes an empty tagged fields marker (0 count).
pub fn write_empty_tagged_fields(buf: &mut impl BufMut) {
    write_uvarint(buf, 0);
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_varint_and_zigzag() {
        let values = [0, 1, -1, 42, -42, 1000, -1000, i32::MAX, i32::MIN];
        for &val in &values {
            let mut buf = BytesMut::new();
            write_varint(&mut buf, val);
            let mut cursor = buf.freeze();
            let decoded = read_varint(&mut cursor).unwrap();
            assert_eq!(decoded, val);
            assert_eq!(cursor.remaining(), 0);
        }
    }

    #[test]
    fn test_strings_standard_and_compact() {
        let mut buf = BytesMut::new();
        write_string(&mut buf, Some("my-topic"));
        write_string(&mut buf, None);
        write_compact_string(&mut buf, Some("compact-topic"));
        write_compact_string(&mut buf, None);

        let mut cursor = buf.freeze();
        assert_eq!(read_string(&mut cursor).unwrap(), Some("my-topic".into()));
        assert_eq!(read_string(&mut cursor).unwrap(), None);
        assert_eq!(
            read_compact_string(&mut cursor).unwrap(),
            Some("compact-topic".into())
        );
        assert_eq!(read_compact_string(&mut cursor).unwrap(), None);
        assert_eq!(cursor.remaining(), 0);
    }

    #[test]
    fn test_arrays() {
        let mut buf = BytesMut::new();
        let list = vec!["alpha", "beta", "gamma"];
        write_array(&mut buf, Some(&list), |b, &s| write_string(b, Some(s)));

        let mut cursor = buf.freeze();
        let decoded = read_array(&mut cursor, |b| Ok(read_string(b)?.unwrap()))
            .unwrap()
            .unwrap();
        assert_eq!(decoded, vec!["alpha", "beta", "gamma"]);
    }
}
