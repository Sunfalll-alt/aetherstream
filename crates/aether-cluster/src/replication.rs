//! Partition replication pipeline and quorum commit tracking.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use crate::error::ClusterError;

/// Progress tracker for an individual remote follower replica.
#[derive(Debug, Clone)]
pub struct FollowerProgress {
    /// Remote follower broker ID.
    pub peer_id: i32,
    /// Highest offset confirmed persisted on this follower.
    pub match_offset: u64,
    /// Next offset to send to this follower.
    pub next_offset: u64,
    /// Timestamp of last successful acknowledge response.
    pub last_ack: Instant,
}

impl FollowerProgress {
    /// Initializes progress tracker starting from zero.
    pub fn new(peer_id: i32) -> Self {
        Self {
            peer_id,
            match_offset: 0,
            next_offset: 0,
            last_ack: Instant::now(),
        }
    }
}

/// Replication coordinator for an individual partition.
pub struct PartitionReplication {
    topic: String,
    partition_id: u32,
    local_node_id: i32,
    high_watermark: u64,
    log_end_offset: u64,
    followers: HashMap<i32, FollowerProgress>,
    commit_waiters: BTreeMap<u64, Vec<oneshot::Sender<Result<(), ClusterError>>>>,
    max_lag_duration: Duration,
}

impl PartitionReplication {
    /// Creates a new partition replication tracker.
    pub fn new(topic: impl Into<String>, partition_id: u32, local_node_id: i32) -> Self {
        Self {
            topic: topic.into(),
            partition_id,
            local_node_id,
            high_watermark: 0,
            log_end_offset: 0,
            followers: HashMap::new(),
            commit_waiters: BTreeMap::new(),
            max_lag_duration: Duration::from_secs(10),
        }
    }

    /// Adds or registers a follower replica.
    pub fn add_follower(&mut self, peer_id: i32) {
        self.followers
            .entry(peer_id)
            .or_insert_with(|| FollowerProgress::new(peer_id));
    }

    /// Removes a follower replica.
    pub fn remove_follower(&mut self, peer_id: i32) {
        self.followers.remove(&peer_id);
    }

    /// Updates leader local LEO.
    pub fn update_local_leo(&mut self, leo: u64) {
        self.log_end_offset = leo;
    }

    /// Records follower acknowledge and recalculates High Watermark across quorum.
    pub fn record_follower_ack(
        &mut self,
        peer_id: i32,
        match_offset: u64,
        quorum_size: usize,
    ) -> u64 {
        let follower = self
            .followers
            .entry(peer_id)
            .or_insert_with(|| FollowerProgress::new(peer_id));
        follower.match_offset = match_offset;
        follower.next_offset = match_offset;
        follower.last_ack = Instant::now();

        self.recalculate_high_watermark(quorum_size)
    }

    /// Recomputes the High Watermark (HW) based on the quorum threshold.
    pub fn recalculate_high_watermark(&mut self, quorum_size: usize) -> u64 {
        // Collect all offsets across local leader and followers
        let mut offsets: Vec<u64> = Vec::with_capacity(self.followers.len() + 1);
        offsets.push(self.log_end_offset);

        for follower in self.followers.values() {
            offsets.push(follower.match_offset);
        }

        // Sort descending to find the highest offset confirmed by a majority
        offsets.sort_unstable_by(|a, b| b.cmp(a));

        if quorum_size > 0 && quorum_size <= offsets.len() {
            let candidate_hw = offsets[quorum_size - 1];
            if candidate_hw > self.high_watermark {
                self.high_watermark = candidate_hw;
                self.notify_waiters(self.high_watermark);
            }
        }

        self.high_watermark
    }

    /// Registers a waiter waiting for quorum commitment up to `target_offset`.
    pub fn register_waiter(
        &mut self,
        target_offset: u64,
        sender: oneshot::Sender<Result<(), ClusterError>>,
    ) {
        if self.high_watermark >= target_offset {
            let _ = sender.send(Ok(()));
            return;
        }

        self.commit_waiters
            .entry(target_offset)
            .or_default()
            .push(sender);
    }

    /// Notifies all waiters whose required offset has been committed.
    fn notify_waiters(&mut self, committed_offset: u64) {
        let to_retain = self.commit_waiters.split_off(&(committed_offset + 1));
        let finished = std::mem::replace(&mut self.commit_waiters, to_retain);

        for (_, senders) in finished {
            for tx in senders {
                let _ = tx.send(Ok(()));
            }
        }
    }

    /// Computes the active In-Sync Replicas (ISR) list.
    pub fn in_sync_replicas(&self) -> Vec<i32> {
        let mut isr = vec![self.local_node_id];
        let now = Instant::now();

        for (&peer_id, follower) in &self.followers {
            // Replica is in-sync if lag time is within configured threshold
            if now.duration_since(follower.last_ack) <= self.max_lag_duration {
                isr.push(peer_id);
            }
        }

        isr
    }

    /// Current committed High Watermark.
    #[inline]
    pub fn high_watermark(&self) -> u64 {
        self.high_watermark
    }

    /// Current Log End Offset.
    #[inline]
    pub fn log_end_offset(&self) -> u64 {
        self.log_end_offset
    }

    /// Topic name.
    #[inline]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Partition ID.
    #[inline]
    pub fn partition_id(&self) -> u32 {
        self.partition_id
    }
}
