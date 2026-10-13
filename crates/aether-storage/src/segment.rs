//! High-performance append-only commit log segment.
//!
//! Each topic partition is divided into fixed-size segments consisting of a `.log` data file
//! and corresponding sparse `.index` files.

use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use tracing::{debug, warn};

use crate::error::StorageError;
use crate::index::{Index, IndexConfig};
use crate::record::Record;

/// Standard file extension for commit log data files.
pub const LOG_FILE_EXT: &str = "log";

/// Configuration settings for commit log segments.
#[derive(Debug, Clone)]
pub struct SegmentConfig {
    /// Maximum byte size of a segment file before rolling to a new segment (e.g. 1 GiB).
    pub max_segment_bytes: u64,
    /// Whether to force fsync/fdatasync to persistent storage on every append.
    pub sync_on_append: bool,
    /// Maximum size in bytes of the preallocated sparse index file (e.g. 10 MiB).
    pub max_index_bytes: usize,
    /// Byte interval between sparse index entries in the log file (e.g. 4096 bytes).
    pub index_interval_bytes: usize,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            max_segment_bytes: 1024 * 1024 * 1024, // 1 GiB
            sync_on_append: false,                 // Default to OS page cache buffering
            max_index_bytes: 10 * 1024 * 1024,     // 10 MiB
            index_interval_bytes: 4096,             // 4 KiB
        }
    }
}

/// Metadata result returned upon successful record append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResult {
    /// The logical partition offset assigned to the appended record.
    pub offset: u64,
    /// Physical byte offset in the `.log` file where the record starts.
    pub physical_position: u64,
    /// Total bytes written to disk for this record.
    pub bytes_written: usize,
}

/// An individual commit log segment file with associated memory-mapped sparse index.
///
/// Thread-safe for concurrent readers via POSIX pread (`read_exact_at`) and memory-mapped
/// binary search index lookups, with synchronized appends and crash-resilient startup recovery.
#[derive(Debug)]
pub struct Segment {
    base_offset: u64,
    next_offset: u64,
    current_size: u64,
    dir: PathBuf,
    log_path: PathBuf,
    index_path: PathBuf,
    file: File,
    index: Index,
    bytes_since_last_index: usize,
    config: SegmentConfig,
}

impl Segment {
    /// Constructs the standard 20-digit zero-padded filename for a segment component.
    ///
    /// Example: `00000000000000000000.log`
    pub fn format_filename(base_offset: u64, extension: &str) -> String {
        format!("{:020}.{}", base_offset, extension)
    }

    /// Creates a new segment starting at `base_offset` in the specified directory.
    pub fn create(
        dir: impl AsRef<Path>,
        base_offset: u64,
        config: SegmentConfig,
    ) -> Result<Self, StorageError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let log_filename = Self::format_filename(base_offset, LOG_FILE_EXT);
        let log_path = dir.join(log_filename);

        let index_filename = Index::format_filename(base_offset);
        let index_path = dir.join(index_filename);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&log_path)?;

        let index_config = IndexConfig {
            max_index_bytes: config.max_index_bytes,
            index_interval_bytes: config.index_interval_bytes,
        };
        let index = Index::create(&index_path, base_offset, index_config)?;

        Ok(Self {
            base_offset,
            next_offset: base_offset,
            current_size: 0,
            dir,
            log_path,
            index_path,
            file,
            index,
            bytes_since_last_index: 0,
            config,
        })
    }

    /// Opens an existing segment, performing crash recovery by scanning valid records
    /// and truncating any uncommitted, corrupted, or partial trailing writes.
    ///
    /// Automatically verifies or rebuilds the corresponding `.index` file from the `.log`.
    pub fn open(
        dir: impl AsRef<Path>,
        base_offset: u64,
        config: SegmentConfig,
    ) -> Result<Self, StorageError> {
        let dir = dir.as_ref().to_path_buf();
        let log_filename = Self::format_filename(base_offset, LOG_FILE_EXT);
        let log_path = dir.join(log_filename);

        let index_filename = Index::format_filename(base_offset);
        let index_path = dir.join(index_filename);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&log_path)?;

        let disk_size = file.metadata()?.len();
        let mut valid_size = 0u64;
        let mut last_offset = None;

        // Recovery scan: validate each record sequentially
        while valid_size < disk_size {
            let remaining = (disk_size - valid_size) as usize;
            if remaining < 4 {
                warn!(
                    "Segment {} has incomplete length header ({} bytes) at offset {}; truncating.",
                    log_path.display(),
                    remaining,
                    valid_size
                );
                break;
            }

            let mut len_buf = [0u8; 4];
            if let Err(e) = file.read_exact_at(&mut len_buf, valid_size) {
                warn!("Recovery read error at {}: {}; truncating.", valid_size, e);
                break;
            }

            let total_frame_len = u32::from_be_bytes(len_buf) as usize;
            let total_record_bytes = 4 + total_frame_len;

            if remaining < total_record_bytes {
                warn!(
                    "Segment {} has truncated record payload (expected {} bytes, found {}) at offset {}; truncating.",
                    log_path.display(),
                    total_record_bytes,
                    remaining,
                    valid_size
                );
                break;
            }

            let mut record_buf = vec![0u8; total_record_bytes];
            if let Err(e) = file.read_exact_at(&mut record_buf, valid_size) {
                warn!("Failed reading full record frame at {}: {}; truncating.", valid_size, e);
                break;
            }

            match Record::decode_from_slice(&record_buf) {
                Ok((record, consumed)) => {
                    last_offset = Some(record.offset);
                    valid_size += consumed as u64;
                }
                Err(err) => {
                    warn!(
                        "Corrupted record encountered at offset {} ({}): truncating segment.",
                        valid_size, err
                    );
                    break;
                }
            }
        }

        // If file had trailing garbage or incomplete write from a crash, truncate to last known clean record
        if valid_size < disk_size {
            warn!(
                "Truncating segment {} from {} bytes down to {} clean bytes",
                log_path.display(),
                disk_size,
                valid_size
            );
            file.set_len(valid_size)?;
            file.sync_data()?;
        }

        let next_offset = match last_offset {
            Some(o) => o + 1,
            None => base_offset,
        };

        // Open or rebuild the sparse index
        let index_config = IndexConfig {
            max_index_bytes: config.max_index_bytes,
            index_interval_bytes: config.index_interval_bytes,
        };

        let index = if index_path.exists() {
            match Index::open(&index_path, base_offset, valid_size, index_config.clone()) {
                Ok(idx) => idx,
                Err(err) => {
                    warn!(
                        "Index {} corrupted or invalid: {}; rebuilding from log {}",
                        index_path.display(),
                        err,
                        log_path.display()
                    );
                    Index::rebuild(&index_path, &log_path, base_offset, index_config)?
                }
            }
        } else {
            warn!(
                "Index {} missing; rebuilding from log {}",
                index_path.display(),
                log_path.display()
            );
            Index::rebuild(&index_path, &log_path, base_offset, index_config)?
        };

        let bytes_since_last_index = match index.last_entry() {
            Some(entry) => (valid_size.saturating_sub(entry.physical_position as u64)) as usize,
            None => valid_size as usize,
        };

        debug!(
            "Opened segment {} (base: {}, next_offset: {}, size: {} bytes, index entries: {})",
            log_path.display(),
            base_offset,
            next_offset,
            valid_size,
            index.entries_count()
        );

        Ok(Self {
            base_offset,
            next_offset,
            current_size: valid_size,
            dir,
            log_path,
            index_path,
            file,
            index,
            bytes_since_last_index,
            config,
        })
    }

    /// Returns the filesystem directory containing this segment.
    #[inline]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Returns the segment's starting logical base offset.
    #[inline]
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Returns the next sequential offset to be assigned.
    #[inline]
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Returns the current physical size of the `.log` file on disk in bytes.
    #[inline]
    pub fn current_size(&self) -> u64 {
        self.current_size
    }

    /// Returns the filesystem path to the segment's `.log` file.
    #[inline]
    pub fn log_path(&self) -> &Path {
        &self.log_path
    }

    /// Returns the filesystem path to the segment's `.index` file.
    #[inline]
    pub fn index_path(&self) -> &Path {
        &self.index_path
    }

    /// Returns a reference to the underlying memory-mapped sparse index.
    #[inline]
    pub fn index(&self) -> &Index {
        &self.index
    }

    /// Appends a new record to the segment, assigning it the current `next_offset`.
    ///
    /// Automatically updates the sparse index when `index_interval_bytes` is reached.
    ///
    /// # Errors
    /// Returns `StorageError::SegmentFull` if appending exceeds `max_segment_bytes`.
    pub fn append(&mut self, record: &mut Record) -> Result<AppendResult, StorageError> {
        let record_size = record.encoded_size();

        if self.current_size + record_size as u64 > self.config.max_segment_bytes {
            return Err(StorageError::SegmentFull {
                current_bytes: self.current_size,
                record_bytes: record_size,
                max_bytes: self.config.max_segment_bytes,
            });
        }

        // Assign next monotonic offset
        record.offset = self.next_offset;

        // Serialize record with CRC32
        let serialized = record.encode();
        let physical_position = self.current_size;

        // Write directly at current_size position (atomic pread/pwrite style)
        self.file.write_all_at(&serialized, physical_position)?;

        // Sparse index update: index first record or when interval threshold is crossed
        let should_index = self.index.entries_count() == 0
            || self.bytes_since_last_index >= self.config.index_interval_bytes;

        if should_index {
            let rel_offset = (record.offset - self.base_offset) as u32;
            self.index.append(rel_offset, physical_position as u32)?;
            self.bytes_since_last_index = 0;
        }

        self.bytes_since_last_index += serialized.len();

        if self.config.sync_on_append {
            self.file.sync_data()?;
            self.index.flush()?;
        }

        self.current_size += serialized.len() as u64;
        self.next_offset += 1;

        Ok(AppendResult {
            offset: record.offset,
            physical_position,
            bytes_written: serialized.len(),
        })
    }

    /// Appends a contiguous batch of records in a single vectorized disk write.
    ///
    /// Pre-computes offsets, serializes all records into a single contiguous buffer,
    /// records sparse index entries, and issues a single atomic `write_all_at` disk write
    /// to minimize kernel syscall transitions.
    pub fn append_batch(&mut self, records: &mut [Record]) -> Result<Vec<AppendResult>, StorageError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        let mut total_batch_bytes = 0usize;
        for rec in records.iter() {
            total_batch_bytes += rec.encoded_size();
        }

        if self.current_size > 0 && self.current_size + total_batch_bytes as u64 > self.config.max_segment_bytes {
            return Err(StorageError::SegmentFull {
                current_bytes: self.current_size,
                record_bytes: total_batch_bytes,
                max_bytes: self.config.max_segment_bytes,
            });
        }

        let mut batch_buf = bytes::BytesMut::with_capacity(total_batch_bytes);
        let mut results = Vec::with_capacity(records.len());
        let mut next_offset = self.next_offset;
        let mut current_pos = self.current_size;
        let mut bytes_since_index = self.bytes_since_last_index;

        for record in records.iter_mut() {
            record.offset = next_offset;
            let start_len = batch_buf.len();
            record.encode_into(&mut batch_buf);
            let rec_len = batch_buf.len() - start_len;

            let should_index = self.index.entries_count() == 0
                || bytes_since_index >= self.config.index_interval_bytes;

            if should_index {
                let rel_offset = (record.offset - self.base_offset) as u32;
                self.index.append(rel_offset, current_pos as u32)?;
                bytes_since_index = 0;
            }

            bytes_since_index += rec_len;

            results.push(AppendResult {
                offset: record.offset,
                physical_position: current_pos,
                bytes_written: rec_len,
            });

            current_pos += rec_len as u64;
            next_offset += 1;
        }

        // Single vectorized atomic disk write
        self.file.write_all_at(&batch_buf, self.current_size)?;

        if self.config.sync_on_append {
            self.file.sync_data()?;
            self.index.flush()?;
        }

        self.current_size = current_pos;
        self.next_offset = next_offset;
        self.bytes_since_last_index = bytes_since_index;

        Ok(results)
    }

    /// Reads a record starting from a given physical byte offset.
    ///
    /// Returns `Ok(None)` if `physical_offset` is at or beyond the current log end.
    /// Returns `Ok(Some((record, next_physical_position)))` upon success.
    pub fn read_record_at(&self, physical_offset: u64) -> Result<Option<(Record, u64)>, StorageError> {
        if physical_offset >= self.current_size {
            return Ok(None);
        }

        let remaining = (self.current_size - physical_offset) as usize;
        if remaining < 4 {
            return Err(StorageError::IncompleteRecord {
                expected: 4,
                available: remaining,
            });
        }

        let mut len_buf = [0u8; 4];
        self.file.read_exact_at(&mut len_buf, physical_offset)?;
        let total_frame_len = u32::from_be_bytes(len_buf) as usize;
        let total_record_bytes = 4 + total_frame_len;

        if remaining < total_record_bytes {
            return Err(StorageError::IncompleteRecord {
                expected: total_record_bytes,
                available: remaining,
            });
        }

        let mut record_buf = vec![0u8; total_record_bytes];
        self.file.read_exact_at(&mut record_buf, physical_offset)?;

        let (record, consumed) = Record::decode_from_slice(&record_buf)?;
        let next_position = physical_offset + consumed as u64;

        Ok(Some((record, next_position)))
    }

    /// Reads a record matching `target_offset` using $O(\log N)$ sparse index lookup
    /// followed by a small sequential page-cache scan.
    ///
    /// # Errors
    /// Returns `StorageError::OffsetOutOfBounds` if `target_offset < self.base_offset`.
    /// Returns `Ok(None)` if `target_offset >= self.next_offset`.
    pub fn read_record_by_offset(
        &self,
        target_offset: u64,
    ) -> Result<Option<(Record, u64)>, StorageError> {
        if target_offset < self.base_offset {
            return Err(StorageError::OffsetOutOfBounds {
                requested: target_offset,
                base_offset: self.base_offset,
                next_offset: self.next_offset,
            });
        }

        if target_offset >= self.next_offset {
            return Ok(None);
        }

        let target_rel_offset = (target_offset - self.base_offset) as u32;

        // Binary search sparse index to jump to the nearest physical position
        let start_pos = match self.index.lookup(target_rel_offset) {
            Some(entry) => entry.physical_position as u64,
            None => 0,
        };

        // Scan forward through at most one indexing interval (e.g. 4 KiB)
        let mut curr_pos = start_pos;
        while curr_pos < self.current_size {
            match self.read_record_at(curr_pos)? {
                Some((record, next_pos)) => {
                    if record.offset == target_offset {
                        return Ok(Some((record, next_pos)));
                    } else if record.offset > target_offset {
                        return Ok(None);
                    }
                    curr_pos = next_pos;
                }
                None => break,
            }
        }

        Ok(None)
    }

    /// Resolves the starting physical byte offset in the log file for `target_offset`.
    ///
    /// Used by zero-copy network transmission (e.g. `sendfile` / `splice`).
    pub fn nearest_physical_position(&self, target_offset: u64) -> Result<u64, StorageError> {
        if target_offset < self.base_offset {
            return Err(StorageError::OffsetOutOfBounds {
                requested: target_offset,
                base_offset: self.base_offset,
                next_offset: self.next_offset,
            });
        }

        let target_rel_offset = (target_offset - self.base_offset) as u32;
        let start_pos = match self.index.lookup(target_rel_offset) {
            Some(entry) => entry.physical_position as u64,
            None => 0,
        };

        let mut curr_pos = start_pos;
        while curr_pos < self.current_size {
            match self.read_record_at(curr_pos)? {
                Some((record, next_pos)) => {
                    if record.offset >= target_offset {
                        return Ok(curr_pos);
                    }
                    curr_pos = next_pos;
                }
                None => break,
            }
        }

        Ok(self.current_size)
    }

    /// Reads a raw byte slice from disk starting at `physical_offset`.
    ///
    /// Useful for pipelined network zero-copy transmission.
    pub fn read_raw_slice(
        &self,
        physical_offset: u64,
        max_bytes: usize,
    ) -> Result<Bytes, StorageError> {
        if physical_offset >= self.current_size {
            return Ok(Bytes::new());
        }

        let available = (self.current_size - physical_offset) as usize;
        let bytes_to_read = max_bytes.min(available);

        let mut buf = vec![0u8; bytes_to_read];
        self.file.read_exact_at(&mut buf, physical_offset)?;

        Ok(Bytes::from(buf))
    }

    /// Flushes any buffered log data and index memory map to persistent storage.
    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.file.sync_data()?;
        self.index.flush()?;
        Ok(())
    }

    /// Trims the sparse index file to its active entries size to reclaim preallocated space.
    pub fn trim_index(&mut self) -> Result<(), StorageError> {
        self.index.trim()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_segment_create_and_append() {
        let dir = tempdir().unwrap();
        let config = SegmentConfig::default();

        let mut segment = Segment::create(dir.path(), 0, config).expect("Segment creation failed");
        assert_eq!(segment.base_offset(), 0);
        assert_eq!(segment.next_offset(), 0);
        assert_eq!(segment.current_size(), 0);

        let mut rec1 = Record::new(
            Some(Bytes::from_static(b"k1")),
            Some(Bytes::from_static(b"value-1")),
        );
        let res1 = segment.append(&mut rec1).expect("Append rec1 failed");
        assert_eq!(res1.offset, 0);
        assert_eq!(res1.physical_position, 0);

        let mut rec2 = Record::new(
            Some(Bytes::from_static(b"k2")),
            Some(Bytes::from_static(b"value-2")),
        );
        let res2 = segment.append(&mut rec2).expect("Append rec2 failed");
        assert_eq!(res2.offset, 1);
        assert_eq!(res2.physical_position, res1.bytes_written as u64);

        assert_eq!(segment.next_offset(), 2);
        assert_eq!(
            segment.current_size(),
            (res1.bytes_written + res2.bytes_written) as u64
        );

        // Read records back sequentially
        let (read_rec1, next_pos1) = segment
            .read_record_at(0)
            .expect("Read rec1 failed")
            .expect("Record 1 should exist");
        assert_eq!(read_rec1.offset, 0);
        assert_eq!(read_rec1.key, Some(Bytes::from_static(b"k1")));
        assert_eq!(read_rec1.value, Some(Bytes::from_static(b"value-1")));
        assert_eq!(next_pos1, res1.bytes_written as u64);

        let (read_rec2, next_pos2) = segment
            .read_record_at(next_pos1)
            .expect("Read rec2 failed")
            .expect("Record 2 should exist");
        assert_eq!(read_rec2.offset, 1);
        assert_eq!(read_rec2.key, Some(Bytes::from_static(b"k2")));
        assert_eq!(read_rec2.value, Some(Bytes::from_static(b"value-2")));
        assert_eq!(next_pos2, segment.current_size());

        // EOF check
        let eof_check = segment.read_record_at(next_pos2).expect("EOF read check");
        assert!(eof_check.is_none());
    }

    #[test]
    fn test_segment_sparse_index_lookups() {
        let dir = tempdir().unwrap();
        // Trigger index entry roughly every ~150 bytes
        let config = SegmentConfig {
            max_segment_bytes: 10 * 1024 * 1024,
            sync_on_append: false,
            max_index_bytes: 1024 * 1024,
            index_interval_bytes: 150,
        };

        let mut segment = Segment::create(dir.path(), 500, config).unwrap();

        // Append 25 records
        for i in 0..25 {
            let mut rec = Record::new(
                Some(Bytes::from(format!("key-{:03}", i))),
                Some(Bytes::from(format!("payload-value-{:03}", i))),
            );
            segment.append(&mut rec).unwrap();
        }

        assert_eq!(segment.next_offset(), 525);
        // Ensure sparse index contains multiple entries
        assert!(segment.index().entries_count() > 1);

        // Look up by logical offset
        for i in 0..25 {
            let offset = 500 + i;
            let (rec, _) = segment
                .read_record_by_offset(offset)
                .expect("Lookup should not error")
                .expect("Record must exist");
            assert_eq!(rec.offset, offset);
            assert_eq!(rec.key, Some(Bytes::from(format!("key-{:03}", i))));
            assert_eq!(
                rec.value,
                Some(Bytes::from(format!("payload-value-{:03}", i)))
            );
        }

        // Out of bounds check: before base offset
        let err_before = segment.read_record_by_offset(499).unwrap_err();
        match err_before {
            StorageError::OffsetOutOfBounds { requested, .. } => assert_eq!(requested, 499),
            other => panic!("Expected OffsetOutOfBounds, got {:?}", other),
        }

        // Beyond next offset check
        let after = segment.read_record_by_offset(525).unwrap();
        assert!(after.is_none());
    }

    #[test]
    fn test_segment_auto_rebuild_missing_index() {
        let dir = tempdir().unwrap();
        let config = SegmentConfig {
            max_segment_bytes: 1024 * 1024,
            sync_on_append: false,
            max_index_bytes: 1024 * 1024,
            index_interval_bytes: 100,
        };

        // 1. Create and write 10 records
        {
            let mut segment = Segment::create(dir.path(), 0, config.clone()).unwrap();
            for i in 0..10 {
                let mut rec = Record::new(
                    Some(Bytes::from(format!("k-{}", i))),
                    Some(Bytes::from(format!("v-{}", i))),
                );
                segment.append(&mut rec).unwrap();
            }
            segment.flush().unwrap();
        }

        // 2. Delete the index file
        let index_path = dir.path().join(Index::format_filename(0));
        assert!(index_path.exists());
        std::fs::remove_file(&index_path).unwrap();
        assert!(!index_path.exists());

        // 3. Re-open segment: it should automatically rebuild index from the log
        {
            let segment = Segment::open(dir.path(), 0, config).expect("Open should succeed");
            assert!(index_path.exists());
            assert!(segment.index().entries_count() > 0);

            // Fast offset lookups should continue working seamlessly
            for i in 0..10 {
                let (rec, _) = segment
                    .read_record_by_offset(i)
                    .unwrap()
                    .expect("Record must exist in rebuilt index");
                assert_eq!(rec.offset, i);
                assert_eq!(rec.key, Some(Bytes::from(format!("k-{}", i))));
            }
        }
    }

    #[test]
    fn test_segment_auto_rebuild_corrupted_index() {
        let dir = tempdir().unwrap();
        let config = SegmentConfig {
            max_segment_bytes: 1024 * 1024,
            sync_on_append: false,
            max_index_bytes: 1024 * 1024,
            index_interval_bytes: 100,
        };

        // 1. Create and write records
        {
            let mut segment = Segment::create(dir.path(), 0, config.clone()).unwrap();
            for i in 0..10 {
                let mut rec = Record::new(
                    Some(Bytes::from(format!("k-{}", i))),
                    Some(Bytes::from(format!("v-{}", i))),
                );
                segment.append(&mut rec).unwrap();
            }
            segment.flush().unwrap();
        }

        // 2. Corrupt index file with bad data
        let index_path = dir.path().join(Index::format_filename(0));
        {
            use std::io::Write;
            let mut f = OpenOptions::new().write(true).open(&index_path).unwrap();
            // Corrupt the first entry
            f.write_all(&[0xFF; 64]).unwrap();
        }

        // 3. Reopen segment: should detect corruption and rebuild cleanly
        {
            let segment = Segment::open(dir.path(), 0, config).expect("Open should rebuild");
            for i in 0..10 {
                let (rec, _) = segment.read_record_by_offset(i).unwrap().unwrap();
                assert_eq!(rec.offset, i);
            }
        }
    }

    #[test]
    fn test_segment_corrupted_trailing_tail_recovery() {
        let dir = tempdir().unwrap();
        let config = SegmentConfig::default();

        let clean_size;
        // Write 2 valid records
        {
            let mut segment =
                Segment::create(dir.path(), 0, config.clone()).expect("Create failed");
            let mut rec1 = Record::new(Some(Bytes::from_static(b"k1")), Some(Bytes::from_static(b"v1")));
            let mut rec2 = Record::new(Some(Bytes::from_static(b"k2")), Some(Bytes::from_static(b"v2")));
            segment.append(&mut rec1).unwrap();
            segment.append(&mut rec2).unwrap();
            clean_size = segment.current_size();
        }

        // Simulate a power-loss midway during a 3rd record append: inject partial corrupt bytes
        let log_path = dir.path().join(Segment::format_filename(0, LOG_FILE_EXT));
        {
            use std::io::Write;
            let mut file = OpenOptions::new().append(true).open(&log_path).unwrap();
            file.write_all(&100u32.to_be_bytes()).unwrap();
            file.write_all(b"partial-corrupt-data").unwrap();
            file.sync_data().unwrap();
        }

        // Re-open: Segment::open must truncate back to clean_size and recover cleanly
        {
            let mut segment = Segment::open(dir.path(), 0, config.clone()).expect("Open should recover");
            assert_eq!(segment.current_size(), clean_size);
            assert_eq!(segment.next_offset(), 2);

            let mut rec3 = Record::new(Some(Bytes::from_static(b"k3")), Some(Bytes::from_static(b"v3")));
            let res = segment.append(&mut rec3).expect("Append after recovery failed");
            assert_eq!(res.offset, 2);
            assert_eq!(segment.next_offset(), 3);
        }
    }

    #[test]
    fn test_segment_capacity_exceeded() {
        let dir = tempdir().unwrap();
        let config = SegmentConfig {
            max_segment_bytes: 100,
            sync_on_append: false,
            max_index_bytes: 1024,
            index_interval_bytes: 4096,
        };

        let mut segment = Segment::create(dir.path(), 0, config).unwrap();
        let mut rec1 = Record::new(
            Some(Bytes::from_static(b"k1")),
            Some(Bytes::from_static(b"v1-hello")),
        );
        segment.append(&mut rec1).unwrap();

        let mut rec2 = Record::new(
            Some(Bytes::from_static(b"k2")),
            Some(Bytes::from_static(b"v2-large-payload-exceeding-segment-capacity-limit-1234567890")),
        );
        let err = segment.append(&mut rec2).unwrap_err();
        match err {
            StorageError::SegmentFull { .. } => {}
            other => panic!("Expected SegmentFull, got {:?}", other),
        }
    }

    #[test]
    fn test_segment_append_batch_vectorized() {
        let dir = tempdir().unwrap();
        let config = SegmentConfig {
            max_segment_bytes: 1024 * 1024,
            sync_on_append: false,
            max_index_bytes: 1024,
            index_interval_bytes: 50,
        };

        let mut segment = Segment::create(dir.path(), 0, config.clone()).unwrap();
        let mut batch: Vec<Record> = (0..10)
            .map(|i| {
                Record::new(
                    Some(Bytes::from(format!("k-{}", i))),
                    Some(Bytes::from(format!("v-{}", i))),
                )
            })
            .collect();

        let results = segment.append_batch(&mut batch).unwrap();
        assert_eq!(results.len(), 10);
        assert_eq!(segment.next_offset(), 10);

        for (i, res) in results.iter().enumerate() {
            assert_eq!(res.offset, i as u64);
        }

        // Verify each record can be read back individually via index
        for i in 0..10 {
            let (rec, _) = segment.read_record_by_offset(i as u64).unwrap().unwrap();
            assert_eq!(rec.offset, i as u64);
            assert_eq!(rec.key.unwrap(), Bytes::from(format!("k-{}", i)));
            assert_eq!(rec.value.unwrap(), Bytes::from(format!("v-{}", i)));
        }

        // Re-open and verify persistence and continuation
        {
            let mut reopened = Segment::open(dir.path(), 0, config).unwrap();
            assert_eq!(reopened.next_offset(), 10);
            let mut more = vec![Record::new(
                Some(Bytes::from_static(b"k-10")),
                Some(Bytes::from_static(b"v-10")),
            )];
            let res2 = reopened.append_batch(&mut more).unwrap();
            assert_eq!(res2[0].offset, 10);
            assert_eq!(reopened.next_offset(), 11);
        }
    }
}
