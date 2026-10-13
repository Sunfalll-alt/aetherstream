//! Central distributed consensus engine orchestrating Raft state, replication pipelines,
//! and inter-broker communication.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use aether_protocol::{BrokerMetadata, ErrorCode, PartitionMetadata};
use aether_storage::Record;
use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Notify};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, trace, warn};

use crate::consensus::{ConsensusState, Role};
use crate::error::ClusterError;
use crate::peer::PeerClient;
use crate::replication::PartitionReplication;
use crate::rpc::{AppendEntriesRequest, ClusterCodec, ClusterMessage};
use crate::topology::ClusterTopology;

/// Interface abstracting partition log operations for consensus replication.
pub trait PartitionProvider: Send + Sync {
    /// Appends records to the local partition log, returning the new Log End Offset.
    fn append_records(
        &self,
        topic: &str,
        partition_id: u32,
        records: &mut [Record],
    ) -> Result<u64, ClusterError>;

    /// Returns the local Log End Offset (LEO) for the partition.
    fn get_leo(&self, topic: &str, partition_id: u32) -> Result<u64, ClusterError>;

    /// Reads records starting from `start_offset` for follower replication.
    fn read_records(
        &self,
        topic: &str,
        partition_id: u32,
        start_offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<Record>, ClusterError>;
}

/// Distributed clustering and consensus engine.
pub struct ClusterEngine {
    local_node_id: i32,
    peer_bind_addr: SocketAddr,
    topology: Arc<RwLock<ClusterTopology>>,
    consensus: Arc<RwLock<ConsensusState>>,
    replications: Arc<RwLock<HashMap<(String, u32), PartitionReplication>>>,
    partition_provider: Arc<dyn PartitionProvider>,
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    election_timeout: Duration,
    heartbeat_interval: Duration,
    rpc_timeout: Duration,
}

impl ClusterEngine {
    /// Creates and initializes a new ClusterEngine instance.
    pub fn new(
        local_node_id: i32,
        peer_bind_addr: SocketAddr,
        client_bind_addr: SocketAddr,
        partition_provider: Arc<dyn PartitionProvider>,
    ) -> Self {
        let topology = Arc::new(RwLock::new(ClusterTopology::new(
            local_node_id,
            client_bind_addr,
            peer_bind_addr,
        )));
        let consensus = Arc::new(RwLock::new(ConsensusState::new(local_node_id)));
        let replications = Arc::new(RwLock::new(HashMap::new()));

        Self {
            local_node_id,
            peer_bind_addr,
            topology,
            consensus,
            replications,
            partition_provider,
            shutdown: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            election_timeout: Duration::from_millis(200),
            heartbeat_interval: Duration::from_millis(50),
            rpc_timeout: Duration::from_millis(150),
        }
    }

    /// Sets custom election and heartbeat timeouts (useful for rapid test failovers).
    pub fn with_timeouts(
        mut self,
        election_timeout: Duration,
        heartbeat_interval: Duration,
        rpc_timeout: Duration,
    ) -> Self {
        self.election_timeout = election_timeout;
        self.heartbeat_interval = heartbeat_interval;
        self.rpc_timeout = rpc_timeout;
        self
    }

    /// Starts the Raft background tasks: peer TCP acceptor, heartbeat ticker, and election monitor.
    pub async fn start(self: &Arc<Self>) -> Result<(), ClusterError> {
        let listener = TcpListener::bind(self.peer_bind_addr).await?;
        let local_addr = listener.local_addr()?;
        info!(
            "ClusterEngine node {} inter-broker listener bound to {}",
            self.local_node_id, local_addr
        );

        // 1. Spawn peer listener
        let engine_clone = Arc::clone(self);
        tokio::spawn(async move {
            engine_clone.run_peer_listener(listener).await;
        });

        // 2. Spawn Raft event loop (heartbeats & elections)
        let engine_clone2 = Arc::clone(self);
        tokio::spawn(async move {
            engine_clone2.run_raft_loop().await;
        });

        Ok(())
    }

    /// Returns the local broker node ID.
    #[inline]
    pub fn node_id(&self) -> i32 {
        self.local_node_id
    }

    /// Returns true if this node is currently the elected Leader.
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.consensus.read().role == Role::Leader
    }

    /// Returns the active leader ID if known.
    #[inline]
    pub fn leader_id(&self) -> Option<i32> {
        self.consensus.read().leader_id
    }

    /// Current Raft election term.
    #[inline]
    pub fn current_term(&self) -> u64 {
        self.consensus.read().current_term
    }

    /// Accesses a snapshot of the cluster topology.
    pub fn topology(&self) -> ClusterTopology {
        self.topology.read().clone()
    }

    /// Returns broker metadata for all known nodes in the cluster.
    pub fn brokers_metadata(&self) -> Vec<BrokerMetadata> {
        self.topology.read().to_broker_metadata()
    }

    /// Returns dynamic Kafka `PartitionMetadata` for a partition, reflecting current Raft leader, term, and ISR.
    pub fn partition_metadata(&self, topic: &str, partition_id: u32) -> PartitionMetadata {
        let topo = self.topology.read();
        let cs = self.consensus.read();
        let mut replicas: Vec<i32> = topo.nodes.keys().copied().collect();
        replicas.sort_unstable();

        let isr = if let Some(rep) = self
            .replications
            .read()
            .get(&(topic.to_string(), partition_id))
        {
            let computed = rep.in_sync_replicas();
            if computed.is_empty() {
                replicas.clone()
            } else {
                computed
            }
        } else {
            replicas.clone()
        };

        let leader_id = cs.leader_id.unwrap_or(-1);
        let error_code = if leader_id == -1 {
            ErrorCode::LeaderNotAvailable
        } else {
            ErrorCode::None
        };

        PartitionMetadata {
            error_code,
            partition_index: partition_id as i32,
            leader_id,
            leader_epoch: Some(cs.current_term as i32),
            replica_nodes: replicas,
            isr_nodes: isr,
            offline_replicas: Vec::new(),
        }
    }

    /// Adds a peer node to the cluster.
    pub fn add_peer(&self, peer_id: i32, client_addr: SocketAddr, peer_addr: SocketAddr) {
        self.topology
            .write()
            .add_node(peer_id, client_addr, peer_addr);
        let mut reps = self.replications.write();
        for rep in reps.values_mut() {
            rep.add_follower(peer_id);
        }
    }

    /// Replicates a newly produced batch across the cluster.
    ///
    /// If `acks == -1`, suspends execution until the batch is confirmed persisted on a quorum.
    pub async fn replicate_produce(
        self: &Arc<Self>,
        topic: &str,
        partition_id: u32,
        records: &mut [Record],
        acks: i16,
        timeout_duration: Duration,
    ) -> Result<u64, ClusterError> {
        // Enforce leader exclusivity for writes
        if !self.is_leader() {
            return Err(ClusterError::NotLeader {
                leader_id: self.leader_id(),
            });
        }

        // 1. Leader appends locally
        let new_leo = self
            .partition_provider
            .append_records(topic, partition_id, records)?;

        // Update replication tracker
        let key = (topic.to_string(), partition_id);
        {
            let peers = self.topology.read().peers();
            let mut reps = self.replications.write();
            let rep = reps.entry(key.clone()).or_insert_with(|| {
                let mut r = PartitionReplication::new(topic, partition_id, self.local_node_id);
                for (pid, _) in &peers {
                    r.add_follower(*pid);
                }
                r
            });
            for (pid, _) in &peers {
                rep.add_follower(*pid);
            }
            rep.update_local_leo(new_leo);
        }

        // 2. If acks == 0 or 1, or cluster size is 1, return immediately
        let quorum_size = self.topology.read().quorum_size();
        if acks == 0 || acks == 1 || quorum_size <= 1 {
            // Trigger asynchronous replication in background
            let engine = Arc::clone(self);
            let t = topic.to_string();
            let p = partition_id;
            let recs = records.to_vec();
            tokio::spawn(async move {
                engine.broadcast_append_entries(&t, p, recs).await;
            });
            return Ok(new_leo);
        }

        // 3. For acks == -1 (or all), wait for quorum commitment
        let (tx, rx) = oneshot::channel();
        {
            let mut reps = self.replications.write();
            if let Some(rep) = reps.get_mut(&key) {
                rep.register_waiter(new_leo, tx);
            }
        }

        // Trigger replication to followers
        let engine = Arc::clone(self);
        let t = topic.to_string();
        let p = partition_id;
        let recs = records.to_vec();
        tokio::spawn(async move {
            engine.broadcast_append_entries(&t, p, recs).await;
        });

        // Await quorum confirmation with timeout
        match tokio::time::timeout(timeout_duration, rx).await {
            Ok(Ok(Ok(()))) => Ok(new_leo),
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(_)) => Err(ClusterError::ChannelClosed),
            Err(_) => {
                let term = self.current_term();
                Err(ClusterError::QuorumTimeout {
                    term,
                    target_offset: new_leo,
                })
            }
        }
    }

    /// Broadcasts AppendEntries RPCs to all peers concurrently and collects acknowledgments.
    async fn broadcast_append_entries(
        self: &Arc<Self>,
        topic: &str,
        partition_id: u32,
        entries: Vec<Record>,
    ) {
        let (term, leader_id) = {
            let cs = self.consensus.read();
            (cs.current_term, self.local_node_id)
        };

        let peers = self.topology.read().peers();
        let key = (topic.to_string(), partition_id);

        let (prev_log_offset, leader_commit) = {
            let reps = self.replications.read();
            if let Some(rep) = reps.get(&key) {
                (
                    rep.log_end_offset().saturating_sub(entries.len() as u64),
                    rep.high_watermark(),
                )
            } else {
                (0, 0)
            }
        };

        let mut join_set = tokio::task::JoinSet::new();

        for (peer_id, peer_addr) in peers {
            let req = AppendEntriesRequest {
                term,
                leader_id,
                topic: topic.to_string(),
                partition_id,
                prev_log_term: term,
                prev_log_offset,
                leader_commit,
                entries: entries.clone(),
            };
            let rpc_timeout = self.rpc_timeout;

            join_set.spawn(async move {
                let res = PeerClient::append_entries(peer_addr, req, rpc_timeout).await;
                (peer_id, res)
            });
        }

        let quorum_size = self.topology.read().quorum_size();

        while let Some(res) = join_set.join_next().await {
            match res {
                Ok((peer_id, Ok(resp))) => {
                    trace!("broadcast got response from peer {}: {:?}", peer_id, resp);
                    if resp.success {
                        let mut reps = self.replications.write();
                        if let Some(rep) = reps.get_mut(&key) {
                            let new_hw =
                                rep.record_follower_ack(peer_id, resp.match_offset, quorum_size);
                            trace!("Recorded ack from peer {}: match_offset={}, new_hw={}, quorum_size={}", peer_id, resp.match_offset, new_hw, quorum_size);
                        }
                    } else if resp.term > term {
                        // Stale leader: step down
                        let mut cs = self.consensus.write();
                        if resp.term > cs.current_term {
                            cs.current_term = resp.term;
                            cs.role = Role::Follower;
                            cs.voted_for = None;
                            cs.leader_id = None;
                        }
                    }
                }
                Ok((peer_id, Err(e))) => {
                    debug!("broadcast RPC error to peer {}: {:?}", peer_id, e);
                }
                Err(join_err) => {
                    warn!("broadcast join error: {:?}", join_err);
                }
            }
        }
    }

    /// Background Raft timer event loop: triggers heartbeats when Leader and starts elections when Follower.
    async fn run_raft_loop(self: &Arc<Self>) {
        let mut interval = tokio::time::interval(self.heartbeat_interval);

        while !self.shutdown.load(Ordering::Relaxed) {
            tokio::select! {
                _ = interval.tick() => {
                    self.tick_raft().await;
                }
                _ = self.shutdown_notify.notified() => {
                    break;
                }
            }
        }
    }

    /// Executes one tick of Raft consensus state machine.
    async fn tick_raft(self: &Arc<Self>) {
        let role = self.consensus.read().role;

        match role {
            Role::Leader => {
                // Leader heartbeats: send empty AppendEntries to keep authority
                let peers = self.topology.read().peers();
                let (term, leader_id) = {
                    let cs = self.consensus.read();
                    (cs.current_term, self.local_node_id)
                };

                for (_peer_id, peer_addr) in peers {
                    let req = AppendEntriesRequest {
                        term,
                        leader_id,
                        topic: String::new(),
                        partition_id: 0,
                        prev_log_term: term,
                        prev_log_offset: 0,
                        leader_commit: 0,
                        entries: Vec::new(),
                    };
                    let rpc_timeout = self.rpc_timeout;
                    tokio::spawn(async move {
                        let _ = PeerClient::append_entries(peer_addr, req, rpc_timeout).await;
                    });
                }
            }
            Role::Follower | Role::Candidate => {
                let elapsed = self.consensus.read().last_heartbeat.elapsed();
                let timeout_duration = self.get_election_timeout();
                if elapsed >= timeout_duration {
                    self.start_election().await;
                }
            }
        }
    }

    /// Computes election timeout with pseudo-random jitter to prevent split-vote deadlocks.
    fn get_election_timeout(&self) -> Duration {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let jitter = (nanos as u64 ^ (self.local_node_id as u64 * 31)) % 100;
        self.election_timeout + Duration::from_millis(jitter)
    }

    /// Initiates a leader election campaign.
    async fn start_election(self: &Arc<Self>) {
        let vote_req = {
            let mut cs = self.consensus.write();
            cs.become_candidate()
        };

        info!(
            "Node {} initiating election for term {}",
            self.local_node_id, vote_req.term
        );

        let peers = self.topology.read().peers();
        let cluster_size = self.topology.read().cluster_size();
        let quorum_size = self.topology.read().quorum_size();

        // If single-node cluster, become leader immediately
        if cluster_size <= 1 {
            self.consensus.write().become_leader();
            info!(
                "Node {} became leader for term {}",
                self.local_node_id, vote_req.term
            );
            return;
        }

        let mut join_set = tokio::task::JoinSet::new();
        for (peer_id, peer_addr) in peers {
            let req = vote_req.clone();
            let rpc_timeout = self.rpc_timeout;
            join_set.spawn(async move {
                let res = PeerClient::request_vote(peer_addr, req, rpc_timeout).await;
                (peer_id, res)
            });
        }

        let mut votes_granted = 1; // Vote for self

        while let Some(res) = join_set.join_next().await {
            if let Ok((_peer_id, Ok(resp))) = res {
                if resp.vote_granted && resp.term == vote_req.term {
                    votes_granted += 1;
                    if votes_granted >= quorum_size {
                        let should_broadcast = {
                            let mut cs = self.consensus.write();
                            if cs.role == Role::Candidate && cs.current_term == vote_req.term {
                                cs.become_leader();
                                info!(
                                    "Node {} won election with {}/{} votes for term {}",
                                    self.local_node_id, votes_granted, cluster_size, vote_req.term
                                );
                                true
                            } else {
                                false
                            }
                        };

                        if should_broadcast {
                            // Immediately send heartbeats to suppress follower elections
                            let peers = self.topology.read().peers();
                            let (term, leader_id) = (vote_req.term, self.local_node_id);
                            for (_peer_id, peer_addr) in peers {
                                let req = AppendEntriesRequest {
                                    term,
                                    leader_id,
                                    topic: String::new(),
                                    partition_id: 0,
                                    prev_log_term: term,
                                    prev_log_offset: 0,
                                    leader_commit: 0,
                                    entries: Vec::new(),
                                };
                                let rpc_timeout = self.rpc_timeout;
                                tokio::spawn(async move {
                                    let _ = PeerClient::append_entries(peer_addr, req, rpc_timeout)
                                        .await;
                                });
                            }
                        }
                        break;
                    }
                } else if resp.term > vote_req.term {
                    let mut cs = self.consensus.write();
                    if resp.term > cs.current_term {
                        cs.current_term = resp.term;
                        cs.role = Role::Follower;
                        cs.voted_for = None;
                        cs.leader_id = None;
                    }
                    break;
                }
            }
        }
    }

    /// Runs the inter-broker TCP peer listener loop.
    async fn run_peer_listener(self: &Arc<Self>, listener: TcpListener) {
        loop {
            tokio::select! {
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok((stream, peer_addr)) => {
                            let engine = Arc::clone(self);
                            tokio::spawn(async move {
                                engine.handle_peer_connection(stream, peer_addr).await;
                            });
                        }
                        Err(e) => {
                            if self.shutdown.load(Ordering::Relaxed) {
                                break;
                            }
                            warn!("Peer accept error: {}", e);
                        }
                    }
                }
                _ = self.shutdown_notify.notified() => {
                    break;
                }
            }
        }
    }

    /// Handles an individual incoming TCP session from a peer broker.
    async fn handle_peer_connection(self: &Arc<Self>, stream: TcpStream, peer_addr: SocketAddr) {
        let mut framed = Framed::new(stream, ClusterCodec::new());

        while let Some(msg_result) = framed.next().await {
            match msg_result {
                Ok(ClusterMessage::VoteRequest(req)) => {
                    let resp = self.consensus.write().handle_vote_request(&req);
                    let _ = framed.send(ClusterMessage::VoteResponse(resp)).await;
                }
                Ok(ClusterMessage::AppendEntriesRequest(mut req)) => {
                    let local_leo = if !req.topic.is_empty() {
                        self.partition_provider
                            .get_leo(&req.topic, req.partition_id)
                            .unwrap_or(0)
                    } else {
                        0
                    };

                    let resp = self
                        .consensus
                        .write()
                        .handle_append_entries(&req, local_leo);

                    // If entries are present and append was accepted, persist to local storage
                    if resp.success && !req.entries.is_empty() {
                        match self.partition_provider.append_records(
                            &req.topic,
                            req.partition_id,
                            &mut req.entries,
                        ) {
                            Ok(new_leo) => {
                                let mut updated_resp = resp;
                                updated_resp.match_offset = new_leo;
                                let _ = framed
                                    .send(ClusterMessage::AppendEntriesResponse(updated_resp))
                                    .await;
                                continue;
                            }
                            Err(e) => {
                                error!("Follower append failed: {}", e);
                            }
                        }
                    }

                    let _ = framed
                        .send(ClusterMessage::AppendEntriesResponse(resp))
                        .await;
                }
                Ok(other) => {
                    trace!(
                        "Unhandled cluster RPC message from {}: {:?}",
                        peer_addr,
                        other
                    );
                }
                Err(err) => {
                    debug!("Peer connection {} closed: {}", peer_addr, err);
                    break;
                }
            }
        }
    }

    /// Gracefully stops the cluster engine and all associated background tasks.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.shutdown_notify.notify_waiters();
    }
}
