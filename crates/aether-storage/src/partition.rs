//! Topic partition commit log manager.
//!
//! Manages ordered segmented log files, active segment append routing, automatic segment
//! rolling at capacity thresholds, cross-segment offset resolution, and retention eviction.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use tracing::{debug, info};

use crate::error::StorageError;
use crate::record::Record;
use crate::segment::{AppendResult, Segment, SegmentConfig, LOG_FILE_EXT};

/// Configuration settings for a partition log.
#[derive(Debug, Clone)]
pub struct PartitionConfig {
    /// Maximum byte size of a segment before rolling to a new segment (e.g. 1 GiB).
    pub max_segment_bytes: u64,
    /// Force fsync/fdatasync on every append.
    pub sync_on_append: bool,
    /// Preallocated sparse index file size (e.g. 10 MiB).
    pub max_index_bytes: usize,
    /// Byte interval between sparse index entries (e.g. 4096 bytes).
    pub index_interval_bytes: usize,
    /// Maximum total retention capacity in bytes for this partition (None = unlimited).
    pub retention_bytes: Option<u64>,
    /// Maximum age in milliseconds for closed segments before eviction (None = unlimited).
    pub retention_ms: Option<u64>,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            max_segment_bytes: 1024 * 1024 * 1024, // 1 GiB
            sync_on_append: false,
            max_index_bytes: 10 * 1024 * 1024,     // 10 MiB
            index_interval_bytes: 4096,             // 4 KiB
            retention_bytes: None,
            retention_ms: None,
        }
    }
}

/// Statistics reported after retention policy execution.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RetentionReport {
    /// Number of closed segments evicted and deleted from disk.
    pub segments_deleted: usize,
    /// Total disk bytes freed.
    pub bytes_reclaimed: u64,
    /// New earliest log start offset after eviction.
    pub new_start_offset: u64,
}

/// A partitioned commit log consisting of ordered historical segments and one writable active segment.
#[derive(Debug)]
pub struct PartitionLog {
    topic: String,
    partition_id: u32,
    dir: PathBuf,
    segments: BTreeMap<u64, Segment>,
    active_base_offset: u64,
    config: PartitionConfig,
}

impl PartitionLog {
    /// Opens an existing partition log or creates a new one at `dir`.
    pub fn open(
        dir: impl AsRef<Path>,
        topic: impl Into<String>,
        partition_id: u32,
        config: PartitionConfig,
    ) -> Result<Self, StorageError> {
        let dir = dir.as_ref().to_path_buf();
        let topic = topic.into();
        std::fs::create_dir_all(&dir)?;

        let seg_cfg = SegmentConfig {
            max_segment_bytes: config.max_segment_bytes,
            sync_on_append: config.sync_on_append,
            max_index_bytes: config.max_index_bytes,
            index_interval_bytes: config.index_interval_bytes,
        };

        // Scan directory for existing {base_offset:020}.log files
        let mut base_offsets = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == LOG_FILE_EXT {
                        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                            if let Ok(base_offset) = stem.parse::<u64>() {
                                base_offsets.push(base_offset);
                            }
                        }
                    }
                }
            }
        }

        base_offsets.sort_unstable();

        let mut segments = BTreeMap::new();
        let active_base_offset = if base_offsets.is_empty() {
            // First time initialization: create active segment 0
            debug!(
                "Initializing empty partition log for {}-{} at {}",
                topic,
                partition_id,
                dir.display()
            );
            let seg = Segment::create(&dir, 0, seg_cfg)?;
            segments.insert(0, seg);
            0
        } else {
            let total = base_offsets.len();
            for (idx, &base) in base_offsets.iter().enumerate() {
                let mut seg = Segment::open(&dir, base, seg_cfg.clone())?;
                // If it's a closed historical segment, trim its index to conserve disk space
                if idx < total - 1 {
                    let _ = seg.trim_index();
                }
                segments.insert(base, seg);
            }
            *base_offsets.last().unwrap()
        };

        info!(
            "Opened partition {}-{} at {}: {} segments, LEO: {}",
            topic,
            partition_id,
            dir.display(),
            segments.len(),
            segments.get(&active_base_offset).unwrap().next_offset()
        );

        Ok(Self {
            topic,
            partition_id,
            dir,
            segments,
            active_base_offset,
            config,
        })
    }

    /// Helper to produce a SegmentConfig matching this partition's settings.
    fn segment_config(&self) -> SegmentConfig {
        SegmentConfig {
            max_segment_bytes: self.config.max_segment_bytes,
            sync_on_append: self.config.sync_on_append,
            max_index_bytes: self.config.max_index_bytes,
            index_interval_bytes: self.config.index_interval_bytes,
        }
    }

    /// Appends a record to the active segment, rolling to a new segment first if capacity is exceeded.
    pub fn append(&mut self, record: &mut Record) -> Result<AppendResult, StorageError> {
        let record_size = record.encoded_size();

        // Check if active segment has data and would exceed max_segment_bytes
        let should_roll = {
            let active = self.segments.get(&self.active_base_offset).unwrap();
            active.current_size() > 0
                && (active.current_size() + record_size as u64 > self.config.max_segment_bytes)
        };

        if should_roll {
            self.roll()?;
        }

        let active = self.segments.get_mut(&self.active_base_offset).unwrap();
        active.append(record)
    }

    /// Rolls the active segment: flushes and trims the current active segment,
    /// then instantiates a new active segment at `next_offset`.
    pub fn roll(&mut self) -> Result<(), StorageError> {
        let new_base_offset = {
            let current_active = self.segments.get_mut(&self.active_base_offset).unwrap();
            current_active.flush()?;
            let _ = current_active.trim_index();
            current_active.next_offset()
        };

        debug!(
            "Rolling partition {}-{} to new active segment at base offset {}",
            self.topic, self.partition_id, new_base_offset
        );

        let new_segment = Segment::create(&self.dir, new_base_offset, self.segment_config())?;
        self.segments.insert(new_base_offset, new_segment);
        self.active_base_offset = new_base_offset;

        Ok(())
    }

    /// Reads a single record matching `target_offset` across all historical and active segments.
    ///
    /// # Errors
    /// Returns `StorageError::PartitionOffsetOutOfBounds` if `target_offset < log_start_offset`.
    /// Returns `Ok(None)` if `target_offset >= log_end_offset`.
    pub fn read_record_by_offset(
        &self,
        target_offset: u64,
    ) -> Result<Option<(Record, u64)>, StorageError> {
        let start_offset = self.log_start_offset();
        let end_offset = self.log_end_offset();

        if target_offset < start_offset {
            return Err(StorageError::PartitionOffsetOutOfBounds {
                requested: target_offset,
                start_offset,
                end_offset,
            });
        }

        if target_offset >= end_offset {
            return Ok(None);
        }

        // Binary search across sorted base offsets in BTreeMap ($O(\log S)$)
        let segment = self
            .segments
            .range(..=target_offset)
            .next_back()
            .map(|(_, seg)| seg)
            .expect("Segment must exist for valid offset in range");

        segment.read_record_by_offset(target_offset)
    }

    /// Reads a contiguous sequence of records starting from `start_offset` up to `max_bytes`,
    /// transparently traversing multiple rolled segment files.
    pub fn read_records_from(
        &self,
        start_offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<Record>, StorageError> {
        let log_start = self.log_start_offset();
        let log_end = self.log_end_offset();

        if start_offset < log_start {
            return Err(StorageError::PartitionOffsetOutOfBounds {
                requested: start_offset,
                start_offset: log_start,
                end_offset: log_end,
            });
        }

        if start_offset >= log_end || max_bytes == 0 {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        let mut curr_offset = start_offset;
        let mut bytes_read = 0usize;

        while curr_offset < log_end && bytes_read < max_bytes {
            let segment = match self.segments.range(..=curr_offset).next_back() {
                Some((_, seg)) => seg,
                None => break,
            };

            // If curr_offset is beyond this segment's next_offset, move to subsequent segment
            if curr_offset >= segment.next_offset() {
                // Find next segment
                match self.segments.range((curr_offset + 1)..).next() {
                    Some((&next_base, _)) => {
                        curr_offset = next_base;
                        continue;
                    }
                    None => break,
                }
            }

            match segment.read_record_by_offset(curr_offset)? {
                Some((record, _)) => {
                    let rec_size = record.encoded_size();
                    bytes_read += rec_size;
                    curr_offset = record.offset + 1;
                    records.push(record);
                }
                None => {
                    // Try to advance to the next segment if any
                    match self.segments.range((curr_offset + 1)..).next() {
                        Some((&next_base, _)) => {
                            curr_offset = next_base;
                        }
                        None => break,
                    }
                }
            }
        }

        Ok(records)
    }

    /// Evaluates and enforces configured time-based and size-based retention policies.
    ///
    /// Never evicts or deletes the active segment.
    pub fn enforce_retention(&mut self) -> Result<RetentionReport, StorageError> {
        let mut report = RetentionReport::default();
        let now = SystemTime::now();

        // 1. Time-based retention eviction
        if let Some(retention_ms) = self.config.retention_ms {
            let max_age = Duration::from_millis(retention_ms);
            let candidate_bases: Vec<u64> = self
                .segments
                .keys()
                .filter(|&&base| base != self.active_base_offset)
                .copied()
                .collect();

            for base in candidate_bases {
                let seg = self.segments.get(&base).unwrap();
                let is_expired = if let Ok(metadata) = seg.log_path().metadata() {
                    if let Ok(mtime) = metadata.modified() {
                        now.duration_since(mtime).unwrap_or_default() >= max_age
                    } else {
                        false
                    }
                } else {
                    false
                };

                if is_expired {
                    let bytes = self.delete_segment(base)?;
                    report.segments_deleted += 1;
                    report.bytes_reclaimed += bytes;
                }
            }
        }

        // 2. Size-based retention eviction
        if let Some(max_total_bytes) = self.config.retention_bytes {
            let mut current_total: u64 = self.total_size_bytes();

            while current_total > max_total_bytes && self.segments.len() > 1 {
                let oldest_base = *self.segments.keys().next().unwrap();
                if oldest_base == self.active_base_offset {
                    break; // Active segment is never deleted
                }

                let reclaimed = self.delete_segment(oldest_base)?;
                current_total = current_total.saturating_sub(reclaimed);
                report.segments_deleted += 1;
                report.bytes_reclaimed += reclaimed;
            }
        }

        report.new_start_offset = self.log_start_offset();
        Ok(report)
    }

    /// Internal helper to drop, unlink and remove an old segment from disk.
    fn delete_segment(&mut self, base_offset: u64) -> Result<u64, StorageError> {
        if let Some(segment) = self.segments.remove(&base_offset) {
            let log_path = segment.log_path().to_path_buf();
            let index_path = segment.index_path().to_path_buf();
            let size = segment.current_size();

            // Explicitly drop segment to unmap memory and close file handles
            drop(segment);

            if log_path.exists() {
                let _ = std::fs::remove_file(log_path);
            }
            if index_path.exists() {
                let _ = std::fs::remove_file(index_path);
            }

            debug!(
                "Evicted segment {} ({} bytes) from partition {}-{}",
                base_offset, size, self.topic, self.partition_id
            );
            Ok(size)
        } else {
            Ok(0)
        }
    }

    /// Returns the logical partition start offset (earliest available offset).
    #[inline]
    pub fn log_start_offset(&self) -> u64 {
        self.segments
            .first_key_value()
            .map(|(base, _)| *base)
            .unwrap_or(0)
    }

    /// Returns the Log End Offset (LEO): the next unwritten monotonic offset.
    #[inline]
    pub fn log_end_offset(&self) -> u64 {
        self.segments
            .get(&self.active_base_offset)
            .map(|seg| seg.next_offset())
            .unwrap_or(0)
    }

    /// Returns the total on-disk size of all log files in this partition in bytes.
    #[inline]
    pub fn total_size_bytes(&self) -> u64 {
        self.segments.values().map(|seg| seg.current_size()).sum()
    }

    /// Returns the number of segments (closed + active) in this partition.
    #[inline]
    pub fn segments_count(&self) -> usize {
        self.segments.len()
    }

    /// Returns a reference to the active segment.
    #[inline]
    pub fn active_segment(&self) -> &Segment {
        self.segments.get(&self.active_base_offset).unwrap()
    }

    /// Returns the topic name.
    #[inline]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition ID.
    #[inline]
    pub fn partition_id(&self) -> u32 {
        self.partition_id
    }

    /// Returns the filesystem directory containing this partition's files.
    #[inline]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Flushes all segments to disk.
    pub fn flush(&mut self) -> Result<(), StorageError> {
        for seg in self.segments.values_mut() {
            seg.flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::tempdir;

    #[test]
    fn test_partition_create_and_append() {
        let dir = tempdir().unwrap();
        let config = PartitionConfig::default();

        let mut partition =
            PartitionLog::open(dir.path(), "orders", 0, config).expect("Open failed");
        assert_eq!(partition.log_start_offset(), 0);
        assert_eq!(partition.log_end_offset(), 0);
        assert_eq!(partition.segments_count(), 1);

        for i in 0..10 {
            let mut rec = Record::new(
                Some(Bytes::from(format!("k-{}", i))),
                Some(Bytes::from(format!("v-{}", i))),
            );
            let res = partition.append(&mut rec).unwrap();
            assert_eq!(res.offset, i);
        }

        assert_eq!(partition.log_start_offset(), 0);
        assert_eq!(partition.log_end_offset(), 10);

        for i in 0..10 {
            let (rec, _) = partition
                .read_record_by_offset(i)
                .unwrap()
                .expect("Record must exist");
            assert_eq!(rec.offset, i);
            assert_eq!(rec.key, Some(Bytes::from(format!("k-{}", i))));
        }
    }

    #[test]
    fn test_partition_automatic_segment_rolling() {
        let dir = tempdir().unwrap();
        // Force roll every ~200 bytes
        let config = PartitionConfig {
            max_segment_bytes: 200,
            sync_on_append: false,
            max_index_bytes: 1024,
            index_interval_bytes: 50,
            retention_bytes: None,
            retention_ms: None,
        };

        let mut partition =
            PartitionLog::open(dir.path(), "telemetry", 1, config).expect("Open failed");

        // Append 25 records (~50-60 bytes each when serialized)
        for i in 0..25 {
            let mut rec = Record::new(
                Some(Bytes::from(format!("device-{:02}", i))),
                Some(Bytes::from(format!("metric-value-payload-{:02}", i))),
            );
            let res = partition.append(&mut rec).unwrap();
            assert_eq!(res.offset, i);
        }

        // Must have rolled into multiple segments
        assert!(partition.segments_count() > 3);
        assert_eq!(partition.log_start_offset(), 0);
        assert_eq!(partition.log_end_offset(), 25);

        // Verify every record is readable across all segments
        for i in 0..25 {
            let (rec, _) = partition
                .read_record_by_offset(i)
                .unwrap()
                .expect("Record must be found across rolled segments");
            assert_eq!(rec.offset, i);
            assert_eq!(rec.key, Some(Bytes::from(format!("device-{:02}", i))));
        }
    }

    #[test]
    fn test_partition_multi_segment_contiguous_reading() {
        let dir = tempdir().unwrap();
        let config = PartitionConfig {
            max_segment_bytes: 200,
            sync_on_append: false,
            max_index_bytes: 1024,
            index_interval_bytes: 50,
            retention_bytes: None,
            retention_ms: None,
        };

        let mut partition = PartitionLog::open(dir.path(), "events", 0, config).unwrap();

        for i in 0..30 {
            let mut rec = Record::new(
                Some(Bytes::from(format!("key-{}", i))),
                Some(Bytes::from(format!("val-{}", i))),
            );
            partition.append(&mut rec).unwrap();
        }

        assert!(partition.segments_count() > 3);

        // Fetch cross-segment batch from offset 5 up to 10000 bytes (should return offsets 5..30)
        let batch = partition.read_records_from(5, 10000).unwrap();
        assert_eq!(batch.len(), 25);
        for (idx, rec) in batch.iter().enumerate() {
            let expected_offset = 5 + idx as u64;
            assert_eq!(rec.offset, expected_offset);
            assert_eq!(rec.key, Some(Bytes::from(format!("key-{}", expected_offset))));
        }

        // Small byte limit fetch (e.g. 150 bytes -> ~2-3 records)
        let small_batch = partition.read_records_from(0, 150).unwrap();
        assert!(!small_batch.is_empty() && small_batch.len() < 5);
        assert_eq!(small_batch[0].offset, 0);
    }

    #[test]
    fn test_partition_reopen_and_resume() {
        let dir = tempdir().unwrap();
        let config = PartitionConfig {
            max_segment_bytes: 200,
            sync_on_append: false,
            max_index_bytes: 1024,
            index_interval_bytes: 50,
            retention_bytes: None,
            retention_ms: None,
        };

        // 1. Write 20 records across multiple segments
        {
            let mut partition =
                PartitionLog::open(dir.path(), "orders", 0, config.clone()).unwrap();
            for i in 0..20 {
                let mut rec = Record::new(
                    Some(Bytes::from(format!("k-{}", i))),
                    Some(Bytes::from(format!("v-{}", i))),
                );
                partition.append(&mut rec).unwrap();
            }
            partition.flush().unwrap();
            assert!(partition.segments_count() > 2);
        }

        // 2. Reopen and verify state preserved
        {
            let mut partition =
                PartitionLog::open(dir.path(), "orders", 0, config.clone()).unwrap();
            assert_eq!(partition.log_start_offset(), 0);
            assert_eq!(partition.log_end_offset(), 20);
            assert!(partition.segments_count() > 2);

            // Append more records
            for i in 20..25 {
                let mut rec = Record::new(
                    Some(Bytes::from(format!("k-{}", i))),
                    Some(Bytes::from(format!("v-{}", i))),
                );
                let res = partition.append(&mut rec).unwrap();
                assert_eq!(res.offset, i);
            }
            assert_eq!(partition.log_end_offset(), 25);

            // Verify full range 0..25
            for i in 0..25 {
                let (rec, _) = partition.read_record_by_offset(i).unwrap().unwrap();
                assert_eq!(rec.offset, i);
            }
        }
    }

    #[test]
    fn test_partition_retention_by_size() {
        let dir = tempdir().unwrap();
        // Configure total retention size to ~400 bytes, with segment roll at ~150 bytes
        let config = PartitionConfig {
            max_segment_bytes: 150,
            sync_on_append: false,
            max_index_bytes: 1024,
            index_interval_bytes: 50,
            retention_bytes: Some(400),
            retention_ms: None,
        };

        let mut partition = PartitionLog::open(dir.path(), "logs", 0, config).unwrap();

        // Write enough data to create 5-6 segments
        for i in 0..30 {
            let mut rec = Record::new(
                Some(Bytes::from(format!("key-{}", i))),
                Some(Bytes::from(format!("val-{}", i))),
            );
            partition.append(&mut rec).unwrap();
        }

        let initial_segments = partition.segments_count();
        assert!(initial_segments >= 4);

        let report = partition.enforce_retention().unwrap();
        assert!(report.segments_deleted > 0);
        assert!(report.bytes_reclaimed > 0);
        assert!(partition.total_size_bytes() <= 400 || partition.segments_count() == 1);
        assert!(partition.log_start_offset() > 0);
        // Active segment must remain intact
        assert_eq!(partition.log_end_offset(), 30);

        // Reading before new start offset must return PartitionOffsetOutOfBounds
        let err = partition.read_record_by_offset(0).unwrap_err();
        match err {
            StorageError::PartitionOffsetOutOfBounds { requested, .. } => {
                assert_eq!(requested, 0);
            }
            other => panic!("Expected PartitionOffsetOutOfBounds, got {:?}", other),
        }

        // Reading at new start offset must succeed
        let (rec, _) = partition
            .read_record_by_offset(partition.log_start_offset())
            .unwrap()
            .unwrap();
        assert_eq!(rec.offset, partition.log_start_offset());
    }

    #[test]
    fn test_partition_retention_by_age() {
        let dir = tempdir().unwrap();
        // Retention age set to 0ms (all closed segments immediately eligible)
        let config = PartitionConfig {
            max_segment_bytes: 150,
            sync_on_append: false,
            max_index_bytes: 1024,
            index_interval_bytes: 50,
            retention_bytes: None,
            retention_ms: Some(0),
        };

        let mut partition = PartitionLog::open(dir.path(), "metrics", 0, config).unwrap();

        for i in 0..20 {
            let mut rec = Record::new(
                Some(Bytes::from(format!("k-{}", i))),
                Some(Bytes::from(format!("v-{}", i))),
            );
            partition.append(&mut rec).unwrap();
        }

        assert!(partition.segments_count() > 1);

        let report = partition.enforce_retention().unwrap();
        assert!(report.segments_deleted > 0);
        // Only active segment must remain!
        assert_eq!(partition.segments_count(), 1);
        assert_eq!(partition.log_end_offset(), 20);
    }
}
