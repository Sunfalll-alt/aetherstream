//! AetherStream Server Layer
//! High-performance async TCP networking and request dispatching.

#![warn(missing_docs)]

/// Server configuration parameters.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Bind address (e.g., "127.0.0.1:9092")
    pub bind_addr: String,
    /// Storage directory path
    pub data_dir: std::path::PathBuf,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9092".to_string(),
            data_dir: std::path::PathBuf::from("/tmp/aetherstream"),
        }
    }
}
