//! AetherStream Protocol Layer
//! Drop-in wire protocol encoder and decoder for Apache Kafka compatibility.

#![warn(missing_docs)]

/// Protocol version definitions and constants.
pub mod version {
    /// Supported Kafka API keys
    #[repr(i16)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ApiKey {
        /// Produce API
        Produce = 0,
        /// Fetch API
        Fetch = 1,
        /// Metadata API
        Metadata = 3,
        /// ApiVersions API
        ApiVersions = 18,
    }
}
