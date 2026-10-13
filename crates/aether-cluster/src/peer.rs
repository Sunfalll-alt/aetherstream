//! Inter-broker TCP networking client.

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::Framed;

use crate::error::ClusterError;
use crate::rpc::{
    AppendEntriesRequest, AppendEntriesResponse, ClusterCodec, ClusterMessage, VoteRequest,
    VoteResponse,
};

/// Client for sending Raft consensus and replication RPCs to a remote peer broker.
pub struct PeerClient;

impl PeerClient {
    /// Sends a VoteRequest to a peer and awaits its VoteResponse.
    pub async fn request_vote(
        peer_addr: SocketAddr,
        req: VoteRequest,
        rpc_timeout: Duration,
    ) -> Result<VoteResponse, ClusterError> {
        let stream = timeout(rpc_timeout, TcpStream::connect(peer_addr))
            .await
            .map_err(|_| ClusterError::PeerUnavailable { peer_id: 0 })??;

        let mut framed = Framed::new(stream, ClusterCodec::new());
        framed.send(ClusterMessage::VoteRequest(req)).await?;

        match timeout(rpc_timeout, framed.next()).await {
            Ok(Some(Ok(ClusterMessage::VoteResponse(resp)))) => Ok(resp),
            Ok(Some(Ok(_))) => Err(ClusterError::Protocol(
                aether_protocol::ProtocolError::InvalidProtocol(
                    "Unexpected RPC message response".to_string(),
                ),
            )),
            Ok(Some(Err(e))) => Err(ClusterError::Protocol(e)),
            Ok(None) => Err(ClusterError::PeerUnavailable { peer_id: 0 }),
            Err(_) => Err(ClusterError::PeerUnavailable { peer_id: 0 }),
        }
    }

    /// Sends an AppendEntriesRequest to a peer and awaits its AppendEntriesResponse.
    pub async fn append_entries(
        peer_addr: SocketAddr,
        req: AppendEntriesRequest,
        rpc_timeout: Duration,
    ) -> Result<AppendEntriesResponse, ClusterError> {
        let stream = timeout(rpc_timeout, TcpStream::connect(peer_addr))
            .await
            .map_err(|_| ClusterError::PeerUnavailable { peer_id: 0 })??;

        let mut framed = Framed::new(stream, ClusterCodec::new());
        framed
            .send(ClusterMessage::AppendEntriesRequest(req))
            .await?;

        match timeout(rpc_timeout, framed.next()).await {
            Ok(Some(Ok(ClusterMessage::AppendEntriesResponse(resp)))) => Ok(resp),
            Ok(Some(Ok(_))) => Err(ClusterError::Protocol(
                aether_protocol::ProtocolError::InvalidProtocol(
                    "Unexpected RPC message response".to_string(),
                ),
            )),
            Ok(Some(Err(e))) => Err(ClusterError::Protocol(e)),
            Ok(None) => Err(ClusterError::PeerUnavailable { peer_id: 0 }),
            Err(_) => Err(ClusterError::PeerUnavailable { peer_id: 0 }),
        }
    }
}
