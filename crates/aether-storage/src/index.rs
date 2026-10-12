//! Memory-mapped sparse index for log segments.
//!
//! Maps logical relative offsets to physical byte positions in a `.log` segment file.
//! Uses `memmap2` for zero-copy memory access and fast $O(\log N)$ binary search lookups.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use memmap2::MmapMut;
use tracing::{debug, warn};

use crate::error::StorageError;
use crate::record::Record;

/// Standard file extension for sparse offset index files.
pub const INDEX_FILE_EXT: &str = "index";

/// Size in bytes of a single index entry: 4 bytes relative offset + 4 bytes physical position.
pub const ENTRY_SIZE: usize = 8;

/// Configuration for the memory-mapped sparse index.
#[derive(Debug, Clone)]
pub struct IndexConfig {
    /// Maximum size in bytes of the preallocated index file (e.g. 10 MiB).
    pub max_index_bytes: usize,
    /// Byte interval between sparse index entries in the log file (e.g. 4096 bytes / 4 KiB).
    pub index_interval_bytes: usize,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            max_index_bytes: 10 * 1024 * 1024, // 10 MiB (~1.31 million entries)
            index_interval_bytes: 4096,         // 4 KiB
        }
    }
}

/// A single entry within the sparse index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    /// Offset relative to segment base offset (`offset - base_offset`).
    pub relative_offset: u32,
    /// Physical byte offset in the corresponding `.log` file.
    pub physical_position: u32,
}

/// Memory-mapped sparse index file.
///
/// Preallocates a fixed capacity file and maps it into userspace via `memmap2`.
/// Lookups execute purely against the OS page cache using binary search without syscalls.
#[derive(Debug)]
pub struct Index {
    path: PathBuf,
    base_offset: u64,
    file: File,
    mmap: MmapMut,
    entries: usize,
    max_entries: usize,
}

impl Index {
    /// Constructs the standard 20-digit zero-padded filename for an index component.
    ///
    /// Example: `00000000000000000000.index`
    pub fn format_filename(base_offset: u64) -> String {
        format!("{:020}.{}", base_offset, INDEX_FILE_EXT)
    }

    /// Creates a brand new sparse index file preallocated to `config.max_index_bytes`.
    pub fn create(
        path: impl AsRef<Path>,
        base_offset: u64,
        config: IndexConfig,
    ) -> Result<Self, StorageError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        let max_index_bytes = config.max_index_bytes.max(ENTRY_SIZE);
        file.set_len(max_index_bytes as u64)?;

        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let max_entries = max_index_bytes / ENTRY_SIZE;

        Ok(Self {
            path,
            base_offset,
            file,
            mmap,
            entries: 0,
            max_entries,
        })
    }

    /// Opens an existing sparse index file, validating entries against the known `log_size`.
    ///
    /// Any entries pointing beyond `log_size` or violating monotonic ordering are discarded.
    pub fn open(
        path: impl AsRef<Path>,
        base_offset: u64,
        log_size: u64,
        config: IndexConfig,
    ) -> Result<Self, StorageError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path)?;

        let disk_len = file.metadata()?.len() as usize;
        let max_index_bytes = config.max_index_bytes.max(ENTRY_SIZE);

        // Ensure file is preallocated to expected capacity
        if disk_len < max_index_bytes {
            file.set_len(max_index_bytes as u64)?;
        }

        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let max_entries = max_index_bytes / ENTRY_SIZE;

        // Scan valid entries bounded by log_size and monotonicity
        let mut valid_entries = 0;
        let mut last_rel_offset = None;
        let mut last_phys_pos = None;

        for i in 0..max_entries {
            let offset = i * ENTRY_SIZE;
            let rel_offset = u32::from_be_bytes(mmap[offset..offset + 4].try_into().unwrap());
            let phys_pos = u32::from_be_bytes(mmap[offset + 4..offset + 8].try_into().unwrap());

            // If log is empty, index must have 0 entries
            if log_size == 0 {
                break;
            }

            // Unwritten entries in preallocated file are zeros
            if i > 0 && rel_offset == 0 && phys_pos == 0 {
                break;
            }

            // Physical position must not exceed current log size
            if phys_pos as u64 >= log_size {
                warn!(
                    "Index entry {} has physical position {} exceeding log size {}; truncating index.",
                    i, phys_pos, log_size
                );
                break;
            }

            // Offsets and physical positions must be strictly monotonically increasing
            if let Some(prev_rel) = last_rel_offset {
                if rel_offset <= prev_rel {
                    warn!(
                        "Non-monotonic relative offset in index at entry {}: {} <= {}; truncating.",
                        i, rel_offset, prev_rel
                    );
                    break;
                }
            }

            if let Some(prev_pos) = last_phys_pos {
                if phys_pos <= prev_pos {
                    warn!(
                        "Non-monotonic physical position in index at entry {}: {} <= {}; truncating.",
                        i, phys_pos, prev_pos
                    );
                    break;
                }
            }

            last_rel_offset = Some(rel_offset);
            last_phys_pos = Some(phys_pos);
            valid_entries += 1;
        }

        debug!(
            "Opened index {} (base: {}, valid entries: {} / max: {})",
            path.display(),
            base_offset,
            valid_entries,
            max_entries
        );

        Ok(Self {
            path,
            base_offset,
            file,
            mmap,
            entries: valid_entries,
            max_entries,
        })
    }

    /// Rebuilds an index from scratch by sequentially scanning the corresponding `.log` file.
    ///
    /// This is the primary crash-recovery path when an index file is missing or corrupted.
    pub fn rebuild(
        index_path: impl AsRef<Path>,
        log_path: impl AsRef<Path>,
        base_offset: u64,
        config: IndexConfig,
    ) -> Result<Self, StorageError> {
        let index_path = index_path.as_ref();
        let log_path = log_path.as_ref();

        debug!(
            "Rebuilding index {} from log {}",
            index_path.display(),
            log_path.display()
        );

        let mut index = Self::create(index_path, base_offset, config.clone())?;

        if !log_path.exists() {
            return Ok(index);
        }

        let log_bytes = std::fs::read(log_path)?;
        let mut curr_pos = 0usize;
        let mut bytes_since_last_index = 0usize;

        while curr_pos < log_bytes.len() {
            let slice = &log_bytes[curr_pos..];
            match Record::decode_from_slice(slice) {
                Ok((record, consumed)) => {
                    let should_index = index.entries == 0
                        || bytes_since_last_index >= config.index_interval_bytes;

                    if should_index {
                        let rel_offset = (record.offset - base_offset) as u32;
                        index.append(rel_offset, curr_pos as u32)?;
                        bytes_since_last_index = 0;
                    }

                    bytes_since_last_index += consumed;
                    curr_pos += consumed;
                }
                Err(err) => {
                    warn!(
                        "Encountered corrupt/incomplete record during index rebuild at {}: {}",
                        curr_pos, err
                    );
                    break;
                }
            }
        }

        index.flush()?;
        Ok(index)
    }

    /// Appends a new sparse index entry (`relative_offset`, `physical_position`).
    ///
    /// # Errors
    /// Returns `StorageError::IndexFull` if maximum capacity is reached.
    /// Returns `StorageError::InvalidFormat` if monotonicity is violated.
    pub fn append(&mut self, relative_offset: u32, physical_position: u32) -> Result<(), StorageError> {
        if self.entries >= self.max_entries {
            return Err(StorageError::IndexFull {
                entries: self.entries,
                max_entries: self.max_entries,
            });
        }

        if self.entries > 0 {
            let last = self.entry_at_unchecked(self.entries - 1);
            if relative_offset <= last.relative_offset {
                return Err(StorageError::InvalidFormat(format!(
                    "Non-monotonic index relative offset: new {} <= previous {}",
                    relative_offset, last.relative_offset
                )));
            }
            if physical_position <= last.physical_position {
                return Err(StorageError::InvalidFormat(format!(
                    "Non-monotonic index physical position: new {} <= previous {}",
                    physical_position, last.physical_position
                )));
            }
        }

        let write_pos = self.entries * ENTRY_SIZE;
        self.mmap[write_pos..write_pos + 4].copy_from_slice(&relative_offset.to_be_bytes());
        self.mmap[write_pos + 4..write_pos + 8].copy_from_slice(&physical_position.to_be_bytes());

        self.entries += 1;
        Ok(())
    }

    /// Performs an $O(\log N)$ binary search to find the nearest physical byte offset
    /// for a given target relative offset.
    ///
    /// Returns the entry with the largest `relative_offset <= target_rel_offset`.
    /// If all entries have `relative_offset > target_rel_offset`, returns the first entry.
    /// Returns `None` if the index has no entries.
    pub fn lookup(&self, target_rel_offset: u32) -> Option<IndexEntry> {
        if self.entries == 0 {
            return None;
        }

        let mut low = 0;
        let mut high = self.entries; // exclusive

        while low < high {
            let mid = low + (high - low) / 2;
            let entry = self.entry_at_unchecked(mid);
            if entry.relative_offset <= target_rel_offset {
                low = mid + 1;
            } else {
                high = mid;
            }
        }

        // low is the first index where relative_offset > target_rel_offset.
        // Therefore, low - 1 is the greatest element with relative_offset <= target_rel_offset.
        if low > 0 {
            Some(self.entry_at_unchecked(low - 1))
        } else {
            Some(self.entry_at_unchecked(0))
        }
    }

    /// Returns the number of valid entries currently recorded in the index.
    #[inline]
    pub fn entries_count(&self) -> usize {
        self.entries
    }

    /// Returns the last entry written to the index, if any.
    #[inline]
    pub fn last_entry(&self) -> Option<IndexEntry> {
        if self.entries == 0 {
            None
        } else {
            Some(self.entry_at_unchecked(self.entries - 1))
        }
    }

    /// Returns the entry at the given entry index, or `None` if out of bounds.
    #[inline]
    pub fn entry_at(&self, idx: usize) -> Option<IndexEntry> {
        if idx < self.entries {
            Some(self.entry_at_unchecked(idx))
        } else {
            None
        }
    }

    /// Reads an entry at `idx` without bounds check.
    #[inline]
    fn entry_at_unchecked(&self, idx: usize) -> IndexEntry {
        let pos = idx * ENTRY_SIZE;
        let rel_offset = u32::from_be_bytes(self.mmap[pos..pos + 4].try_into().unwrap());
        let phys_pos = u32::from_be_bytes(self.mmap[pos + 4..pos + 8].try_into().unwrap());
        IndexEntry {
            relative_offset: rel_offset,
            physical_position: phys_pos,
        }
    }

    /// Flushes dirty mmap pages and file metadata to persistent storage.
    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.mmap.flush()?;
        self.file.sync_data()?;
        Ok(())
    }

    /// Truncates the underlying index file to actual active entries size (`entries * ENTRY_SIZE`).
    ///
    /// Invoked during segment roll/close to reclaim preallocated space.
    pub fn trim(&mut self) -> Result<(), StorageError> {
        let active_bytes = (self.entries * ENTRY_SIZE).max(ENTRY_SIZE);
        self.mmap.flush()?;
        self.file.set_len(active_bytes as u64)?;
        self.mmap = unsafe { MmapMut::map_mut(&self.file)? };
        self.max_entries = active_bytes / ENTRY_SIZE;
        Ok(())
    }

    /// Returns the segment base offset associated with this index.
    #[inline]
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Returns the path to this index file.
    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_index_create_and_append() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let config = IndexConfig {
            max_index_bytes: 1024,
            index_interval_bytes: 4096,
        };

        let mut index = Index::create(&path, 0, config).expect("Create index failed");
        assert_eq!(index.entries_count(), 0);
        assert_eq!(index.last_entry(), None);

        index.append(0, 0).expect("Append entry 0 failed");
        index.append(10, 500).expect("Append entry 1 failed");
        index.append(25, 1200).expect("Append entry 2 failed");

        assert_eq!(index.entries_count(), 3);
        assert_eq!(
            index.entry_at(0),
            Some(IndexEntry {
                relative_offset: 0,
                physical_position: 0
            })
        );
        assert_eq!(
            index.entry_at(1),
            Some(IndexEntry {
                relative_offset: 10,
                physical_position: 500
            })
        );
        assert_eq!(
            index.entry_at(2),
            Some(IndexEntry {
                relative_offset: 25,
                physical_position: 1200
            })
        );
        assert_eq!(index.entry_at(3), None);
    }

    #[test]
    fn test_index_binary_search_exact_and_floor() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let config = IndexConfig::default();

        let mut index = Index::create(&path, 0, config).unwrap();
        // Insert sparse points:
        // offset 0   -> byte 0
        // offset 10  -> byte 1000
        // offset 25  -> byte 2500
        // offset 50  -> byte 5000
        // offset 100 -> byte 10000
        index.append(0, 0).unwrap();
        index.append(10, 1000).unwrap();
        index.append(25, 2500).unwrap();
        index.append(50, 5000).unwrap();
        index.append(100, 10000).unwrap();

        // Exact matches
        assert_eq!(index.lookup(0).unwrap().physical_position, 0);
        assert_eq!(index.lookup(10).unwrap().physical_position, 1000);
        assert_eq!(index.lookup(25).unwrap().physical_position, 2500);
        assert_eq!(index.lookup(50).unwrap().physical_position, 5000);
        assert_eq!(index.lookup(100).unwrap().physical_position, 10000);

        // Floor matches (between sparse entries)
        assert_eq!(index.lookup(5).unwrap().physical_position, 0);
        assert_eq!(index.lookup(9).unwrap().physical_position, 0);
        assert_eq!(index.lookup(11).unwrap().physical_position, 1000);
        assert_eq!(index.lookup(24).unwrap().physical_position, 1000);
        assert_eq!(index.lookup(26).unwrap().physical_position, 2500);
        assert_eq!(index.lookup(49).unwrap().physical_position, 2500);
        assert_eq!(index.lookup(75).unwrap().physical_position, 5000);
        assert_eq!(index.lookup(99).unwrap().physical_position, 5000);

        // Beyond last entry
        assert_eq!(index.lookup(150).unwrap().physical_position, 10000);
        assert_eq!(index.lookup(1000).unwrap().physical_position, 10000);
    }

    #[test]
    fn test_index_empty_and_single_entry() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let config = IndexConfig::default();

        let mut index = Index::create(&path, 0, config).unwrap();
        assert_eq!(index.lookup(0), None);
        assert_eq!(index.lookup(100), None);

        index.append(5, 500).unwrap();
        assert_eq!(
            index.lookup(0),
            Some(IndexEntry {
                relative_offset: 5,
                physical_position: 500
            })
        );
        assert_eq!(
            index.lookup(5),
            Some(IndexEntry {
                relative_offset: 5,
                physical_position: 500
            })
        );
        assert_eq!(
            index.lookup(50),
            Some(IndexEntry {
                relative_offset: 5,
                physical_position: 500
            })
        );
    }

    #[test]
    fn test_index_monotonicity_enforcement() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let config = IndexConfig::default();

        let mut index = Index::create(&path, 0, config).unwrap();
        index.append(10, 100).unwrap();

        // Equal relative offset should fail
        let err1 = index.append(10, 200).unwrap_err();
        match err1 {
            StorageError::InvalidFormat(msg) => assert!(msg.contains("relative offset")),
            other => panic!("Expected InvalidFormat, got {:?}", other),
        }

        // Lesser relative offset should fail
        let err2 = index.append(5, 200).unwrap_err();
        match err2 {
            StorageError::InvalidFormat(msg) => assert!(msg.contains("relative offset")),
            other => panic!("Expected InvalidFormat, got {:?}", other),
        }

        // Equal or lesser physical position should fail
        let err3 = index.append(20, 100).unwrap_err();
        match err3 {
            StorageError::InvalidFormat(msg) => assert!(msg.contains("physical position")),
            other => panic!("Expected InvalidFormat, got {:?}", other),
        }
    }

    #[test]
    fn test_index_trim() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let config = IndexConfig {
            max_index_bytes: 1024 * 1024, // 1 MiB
            index_interval_bytes: 4096,
        };

        let mut index = Index::create(&path, 0, config).unwrap();
        index.append(0, 0).unwrap();
        index.append(10, 500).unwrap();

        assert_eq!(std::fs::metadata(&path).unwrap().len(), 1024 * 1024);

        index.trim().expect("Trim should succeed");
        // Active entries: 2 * 8 = 16 bytes
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 16);
        assert_eq!(index.entries_count(), 2);
        assert_eq!(index.lookup(10).unwrap().physical_position, 500);
    }
}
