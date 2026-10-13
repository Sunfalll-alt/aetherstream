//! Async TCP networking server and connection multiplexer.

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use aether_protocol::{InboundRequest, KafkaCodec};
use aether_storage::PartitionConfig;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

use crate::error::ServerError;
use crate::handler::handle_request;
use crate::state::BrokerState;

/// Server configuration parameters.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Bind address (e.g. "127.0.0.1:9092").
    pub bind_addr: String,
    /// Storage root directory path.
    pub data_dir: PathBuf,
    /// Broker cluster node ID.
    pub node_id: i32,
    /// Advertised host for client metadata discovery.
    pub advertised_host: String,
    /// Advertised port for client metadata discovery.
    pub advertised_port: i32,
    /// Storage engine configuration settings.
    pub partition_config: PartitionConfig,
    /// Inter-broker Raft clustering bind address (e.g. "127.0.0.1:9093").
    pub cluster_bind_addr: Option<String>,
    /// Cluster peers: (node_id, client_addr, peer_addr).
    pub cluster_peers: Vec<(i32, SocketAddr, SocketAddr)>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9092".to_string(),
            data_dir: PathBuf::from("/tmp/aetherstream"),
            node_id: 1,
            advertised_host: "127.0.0.1".to_string(),
            advertised_port: 9092,
            partition_config: PartitionConfig::default(),
            cluster_bind_addr: None,
            cluster_peers: Vec::new(),
        }
    }
}

/// AetherStream Broker TCP Server.
pub struct AetherServer {
    config: ServerConfig,
    state: Arc<BrokerState>,
}

impl AetherServer {
    /// Initializes a new broker server instance.
    pub fn new(config: ServerConfig) -> Result<Self, ServerError> {
        let state = Arc::new(BrokerState::new(
            config.node_id,
            config.advertised_host.clone(),
            config.advertised_port,
            &config.data_dir,
            config.partition_config.clone(),
        )?);

        if let Some(ref peer_bind) = config.cluster_bind_addr {
            let peer_addr: SocketAddr = peer_bind.parse().map_err(|e| {
                ServerError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
            })?;
            let client_addr: SocketAddr =
                format!("{}:{}", config.advertised_host, config.advertised_port)
                    .parse()
                    .map_err(|e| {
                        ServerError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
                    })?;

            let cluster_engine = Arc::new(aether_cluster::ClusterEngine::new(
                config.node_id,
                peer_addr,
                client_addr,
                Arc::clone(&state) as Arc<dyn aether_cluster::PartitionProvider>,
            ));

            for &(peer_id, peer_client_addr, peer_raft_addr) in &config.cluster_peers {
                cluster_engine.add_peer(peer_id, peer_client_addr, peer_raft_addr);
            }

            state.set_cluster(cluster_engine);
        }

        Ok(Self { config, state })
    }

    /// Attaches an existing cluster engine to the server instance.
    pub fn with_cluster(self, cluster: Arc<aether_cluster::ClusterEngine>) -> Self {
        self.state.set_cluster(cluster);
        self
    }

    /// Returns a reference to the broker state.
    pub fn state(&self) -> &Arc<BrokerState> {
        &self.state
    }

    /// Runs the server binding to `config.bind_addr` until `shutdown_signal` is resolved.
    pub async fn run(
        &self,
        shutdown_signal: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), ServerError> {
        let listener = TcpListener::bind(&self.config.bind_addr).await?;
        self.run_with_listener(listener, shutdown_signal).await
    }

    /// Runs the server using an existing `TcpListener` (ideal for test ephemeral port binding).
    pub async fn run_with_listener(
        &self,
        listener: TcpListener,
        shutdown_signal: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), ServerError> {
        let local_addr = listener.local_addr()?;
        info!(
            "AetherStream Broker node {} listening on tcp://{}",
            self.config.node_id, local_addr
        );

        // Start clustering engine if active
        if let Some(cluster) = self.state.cluster() {
            cluster.start().await?;
        }

        tokio::pin!(shutdown_signal);

        loop {
            tokio::select! {
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok((stream, peer_addr)) => {
                            debug!("Accepted TCP connection from {}", peer_addr);
                            let state = Arc::clone(&self.state);
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(stream, peer_addr, state).await {
                                    debug!("Connection {} closed: {}", peer_addr, e);
                                }
                            });
                        }
                        Err(e) => {
                            warn!("TCP accept failure: {}", e);
                        }
                    }
                }
                _ = &mut shutdown_signal => {
                    info!("Shutdown signal received. Stopping TCP acceptor...");
                    break;
                }
            }
        }

        // Stop clustering engine if active
        if let Some(cluster) = self.state.cluster() {
            cluster.shutdown();
        }

        info!("AetherStream Broker stopped gracefully.");
        Ok(())
    }
}

/// Handles an individual TCP client connection session.
async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    state: Arc<BrokerState>,
) -> Result<(), ServerError> {
    // Disable Nagle's algorithm for low p99 latency
    let _ = stream.set_nodelay(true);

    let mut framed = Framed::new(stream, KafkaCodec::default());

    while let Some(frame_result) = framed.next().await {
        match frame_result {
            Ok(frame) => {
                let inbound = InboundRequest::from_frame(frame)?;
                let resp_bytes = handle_request(Arc::clone(&state), inbound).await?;
                if !resp_bytes.is_empty() {
                    framed.send(resp_bytes).await?;
                }
            }
            Err(err) => {
                error!("Protocol framing error from {}: {}", peer_addr, err);
                return Err(ServerError::Protocol(err));
            }
        }
    }

    Ok(())
}
