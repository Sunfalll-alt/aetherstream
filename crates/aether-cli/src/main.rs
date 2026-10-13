//! AetherStream CLI entrypoint and broker daemon.

use std::path::PathBuf;

use aether_server::{AetherServer, ServerConfig};
use aether_storage::PartitionConfig;
use anyhow::Result;
use tracing::info;

struct CliConfig {
    host: String,
    port: i32,
    data_dir: PathBuf,
    node_id: i32,
    cluster_bind: Option<String>,
    cluster_peers: Vec<(i32, std::net::SocketAddr, std::net::SocketAddr)>,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 9092,
            data_dir: PathBuf::from("/tmp/aetherstream"),
            node_id: 1,
            cluster_bind: None,
            cluster_peers: Vec::new(),
        }
    }
}

fn parse_args() -> Result<CliConfig> {
    let mut config = CliConfig::default();
    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--host" => {
                if let Some(val) = args.next() {
                    config.host = val;
                }
            }
            "--port" => {
                if let Some(val) = args.next() {
                    config.port = val.parse()?;
                }
            }
            "--data-dir" => {
                if let Some(val) = args.next() {
                    config.data_dir = PathBuf::from(val);
                }
            }
            "--node-id" => {
                if let Some(val) = args.next() {
                    config.node_id = val.parse()?;
                }
            }
            "--cluster-bind" => {
                if let Some(val) = args.next() {
                    config.cluster_bind = Some(val);
                }
            }
            "-h" | "--help" => {
                println!(
                    "AetherStream Broker v0.1.0\n\n\
                    USAGE:\n    aether [OPTIONS]\n\n\
                    OPTIONS:\n\
                        --host <HOST>            Bind / advertised host (default: 127.0.0.1)\n\
                        --port <PORT>            Bind / advertised port (default: 9092)\n\
                        --data-dir <DIR>         Commit log data directory (default: /tmp/aetherstream)\n\
                        --node-id <ID>           Broker node ID (default: 1)\n\
                        --cluster-bind <ADDR>    Inter-broker Raft clustering bind address (e.g. 127.0.0.1:9093)\n\
                        -h, --help               Print help information\n"
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("Unknown argument: {}", other);
                std::process::exit(1);
            }
        }
    }

    Ok(config)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Structured observability logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli_config = parse_args()?;

    info!("Starting AetherStream Broker v0.1.0...");
    info!("Data directory: {}", cli_config.data_dir.display());

    let server_config = ServerConfig {
        bind_addr: format!("{}:{}", cli_config.host, cli_config.port),
        data_dir: cli_config.data_dir,
        node_id: cli_config.node_id,
        advertised_host: cli_config.host,
        advertised_port: cli_config.port,
        partition_config: PartitionConfig::default(),
        cluster_bind_addr: cli_config.cluster_bind,
        cluster_peers: cli_config.cluster_peers,
    };

    let server = AetherServer::new(server_config)?;

    // Handle SIGINT (Ctrl+C)
    let shutdown_signal = async {
        let _ = tokio::signal::ctrl_c().await;
        info!("Received interrupt signal (SIGINT), initiating graceful shutdown...");
    };

    server.run(shutdown_signal).await?;
    Ok(())
}
