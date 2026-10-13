//! In-memory broker state and partition registry.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use aether_storage::{PartitionConfig, PartitionLog};
use parking_lot::RwLock;
use tracing::{debug, info};

use crate::error::ServerError;

/// Type alias for the partition registry map.
pub type PartitionMap = HashMap<(String, u32), Arc<RwLock<PartitionLog>>>;

/// In-memory state of the broker managing active topics and partitions.
pub struct BrokerState {
    /// Unique cluster node identifier.
    pub node_id: i32,
    /// Advertised host or IP for client discovery.
    pub host: String,
    /// Advertised listener port.
    pub port: i32,
    /// Root filesystem directory for partition commit logs.
    pub data_dir: PathBuf,
    /// Storage engine configuration parameters for created partitions.
    pub partition_config: PartitionConfig,
    /// Active partition log instances indexed by `(topic, partition_id)`.
    partitions: RwLock<PartitionMap>,
    /// Optional distributed clustering engine managing consensus & replication.
    cluster: RwLock<Option<Arc<aether_cluster::ClusterEngine>>>,
}

impl std::fmt::Debug for BrokerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerState")
            .field("node_id", &self.node_id)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("data_dir", &self.data_dir)
            .field("partition_config", &self.partition_config)
            .field("partitions_count", &self.partitions.read().len())
            .field("cluster_active", &self.cluster.read().is_some())
            .finish()
    }
}

impl BrokerState {
    /// Initializes a new broker state, automatically discovering existing partitions in `data_dir`.
    pub fn new(
        node_id: i32,
        host: impl Into<String>,
        port: i32,
        data_dir: impl AsRef<Path>,
        partition_config: PartitionConfig,
    ) -> Result<Self, ServerError> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let host = host.into();
        std::fs::create_dir_all(&data_dir)?;

        let mut partitions_map = HashMap::new();

        // Scan data_dir for existing partition directories ({topic}-{partition_id})
        if let Ok(entries) = std::fs::read_dir(&data_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                        if let Some(last_dash) = dir_name.rfind('-') {
                            let topic = &dir_name[..last_dash];
                            if let Ok(partition_id) = dir_name[last_dash + 1..].parse::<u32>() {
                                debug!(
                                    "Discovered existing partition {}-{} on disk",
                                    topic, partition_id
                                );
                                match PartitionLog::open(
                                    &path,
                                    topic,
                                    partition_id,
                                    partition_config.clone(),
                                ) {
                                    Ok(part_log) => {
                                        partitions_map.insert(
                                            (topic.to_string(), partition_id),
                                            Arc::new(RwLock::new(part_log)),
                                        );
                                    }
                                    Err(err) => {
                                        tracing::warn!(
                                            "Failed to recover partition at {}: {}",
                                            path.display(),
                                            err
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        info!(
            "Broker node {} initialized at {}:{}, loaded {} partitions from disk",
            node_id,
            host,
            port,
            partitions_map.len()
        );

        Ok(Self {
            node_id,
            host,
            port,
            data_dir,
            partition_config,
            partitions: RwLock::new(partitions_map),
            cluster: RwLock::new(None),
        })
    }

    /// Retrieves an existing partition log or dynamically creates a new one.
    pub fn get_or_create_partition(
        &self,
        topic: &str,
        partition_id: u32,
    ) -> Result<Arc<RwLock<PartitionLog>>, ServerError> {
        // Fast path: read lock
        {
            let map = self.partitions.read();
            if let Some(part) = map.get(&(topic.to_string(), partition_id)) {
                return Ok(Arc::clone(part));
            }
        }

        // Slow path: write lock to instantiate new partition
        let mut map = self.partitions.write();
        if let Some(part) = map.get(&(topic.to_string(), partition_id)) {
            return Ok(Arc::clone(part));
        }

        let partition_dir = self.data_dir.join(format!("{}-{}", topic, partition_id));
        let part_log = PartitionLog::open(
            &partition_dir,
            topic,
            partition_id,
            self.partition_config.clone(),
        )?;

        let shared_part = Arc::new(RwLock::new(part_log));
        map.insert((topic.to_string(), partition_id), Arc::clone(&shared_part));

        info!(
            "Dynamically created partition {}-{} at {}",
            topic,
            partition_id,
            partition_dir.display()
        );

        Ok(shared_part)
    }

    /// Retrieves an existing partition if it is currently registered.
    pub fn get_partition(
        &self,
        topic: &str,
        partition_id: u32,
    ) -> Option<Arc<RwLock<PartitionLog>>> {
        let map = self.partitions.read();
        map.get(&(topic.to_string(), partition_id)).cloned()
    }

    /// Lists all registered topics and their corresponding partition indices.
    pub fn list_topics(&self) -> Vec<(String, Vec<u32>)> {
        let map = self.partitions.read();
        let mut grouped: HashMap<String, Vec<u32>> = HashMap::new();

        for (topic, part_id) in map.keys() {
            grouped.entry(topic.clone()).or_default().push(*part_id);
        }

        let mut result = Vec::new();
        for (topic, mut parts) in grouped {
            parts.sort_unstable();
            result.push((topic, parts));
        }
        result.sort_by(|a, b| a.0.cmp(&b.0));
        result
    }

    /// Checks if a topic exists.
    pub fn topic_exists(&self, topic: &str) -> bool {
        let map = self.partitions.read();
        map.keys().any(|(t, _)| t == topic)
    }

    /// Associates a distributed clustering consensus engine with this broker state.
    pub fn set_cluster(&self, cluster: Arc<aether_cluster::ClusterEngine>) {
        *self.cluster.write() = Some(cluster);
    }

    /// Returns a reference to the active cluster engine, if enabled.
    pub fn cluster(&self) -> Option<Arc<aether_cluster::ClusterEngine>> {
        self.cluster.read().clone()
    }
}

impl aether_cluster::PartitionProvider for BrokerState {
    fn append_records(
        &self,
        topic: &str,
        partition_id: u32,
        records: &mut [aether_storage::Record],
    ) -> Result<u64, aether_cluster::ClusterError> {
        let part = self
            .get_or_create_partition(topic, partition_id)
            .map_err(|e| aether_cluster::ClusterError::General(e.to_string()))?;
        let mut part = part.write();
        let _ = part
            .append_batch(records)
            .map_err(aether_cluster::ClusterError::Storage)?;
        Ok(part.log_end_offset())
    }

    fn get_leo(&self, topic: &str, partition_id: u32) -> Result<u64, aether_cluster::ClusterError> {
        let part = self
            .get_or_create_partition(topic, partition_id)
            .map_err(|e| aether_cluster::ClusterError::General(e.to_string()))?;
        let part = part.read();
        Ok(part.log_end_offset())
    }

    fn read_records(
        &self,
        topic: &str,
        partition_id: u32,
        start_offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<aether_storage::Record>, aether_cluster::ClusterError> {
        let part = self
            .get_or_create_partition(topic, partition_id)
            .map_err(|e| aether_cluster::ClusterError::General(e.to_string()))?;
        let part = part.read();
        let records = part
            .read_records_from(start_offset, max_bytes)
            .map_err(aether_cluster::ClusterError::Storage)?;
        Ok(records)
    }
}
