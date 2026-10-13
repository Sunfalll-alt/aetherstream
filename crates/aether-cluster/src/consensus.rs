//! Raft consensus state machine logic.

use std::time::Instant;

use crate::rpc::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};

/// Raft consensus node role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Passively replicating from active leader.
    Follower,
    /// Actively seeking votes during election.
    Candidate,
    /// Elected leader serving writes and replicating to followers.
    Leader,
}

/// In-memory Raft state tracking terms, roles, and log pointers.
#[derive(Debug, Clone)]
pub struct ConsensusState {
    /// Local node ID.
    pub node_id: i32,
    /// Current election term.
    pub current_term: u64,
    /// Node ID voted for in the current term, if any.
    pub voted_for: Option<i32>,
    /// Current role.
    pub role: Role,
    /// Known active leader node ID, if any.
    pub leader_id: Option<i32>,
    /// Timestamp of last received valid heartbeat / append RPC.
    pub last_heartbeat: Instant,
    /// Term of the last log entry persisted locally.
    pub last_log_term: u64,
    /// Monotonic offset of the last log entry persisted locally.
    pub last_log_offset: u64,
    /// Committed offset (High Watermark).
    pub commit_offset: u64,
}

impl ConsensusState {
    /// Initializes new consensus state as a Follower at term 0.
    pub fn new(node_id: i32) -> Self {
        Self {
            node_id,
            current_term: 0,
            voted_for: None,
            role: Role::Follower,
            leader_id: None,
            last_heartbeat: Instant::now(),
            last_log_term: 0,
            last_log_offset: 0,
            commit_offset: 0,
        }
    }

    /// Evaluates incoming VoteRequest according to Raft safety specifications.
    pub fn handle_vote_request(&mut self, req: &VoteRequest) -> VoteResponse {
        if req.term > self.current_term {
            self.current_term = req.term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.leader_id = None;
        }

        let mut vote_granted = false;

        if req.term == self.current_term {
            let can_vote = match self.voted_for {
                None => true,
                Some(id) => id == req.candidate_id,
            };

            // Candidate log must be at least as up-to-date as receiver log
            let log_ok = req.last_log_term > self.last_log_term
                || (req.last_log_term == self.last_log_term
                    && req.last_log_offset >= self.last_log_offset);

            if can_vote && log_ok {
                vote_granted = true;
                self.voted_for = Some(req.candidate_id);
                self.last_heartbeat = Instant::now();
            }
        }

        VoteResponse {
            term: self.current_term,
            vote_granted,
        }
    }

    /// Evaluates incoming AppendEntriesRequest according to Raft log alignment rules.
    pub fn handle_append_entries(
        &mut self,
        req: &AppendEntriesRequest,
        local_leo: u64,
    ) -> AppendEntriesResponse {
        if req.term > self.current_term {
            self.current_term = req.term;
            self.role = Role::Follower;
            self.voted_for = None;
        }

        if req.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                follower_id: self.node_id,
                topic: req.topic.clone(),
                partition_id: req.partition_id,
                success: false,
                match_offset: local_leo,
            };
        }

        // Valid leader recognized
        self.leader_id = Some(req.leader_id);
        self.role = Role::Follower;
        self.last_heartbeat = Instant::now();

        // Log consistency check: local log must contain entry at prev_log_offset
        if req.prev_log_offset > 0 && local_leo < req.prev_log_offset {
            return AppendEntriesResponse {
                term: self.current_term,
                follower_id: self.node_id,
                topic: req.topic.clone(),
                partition_id: req.partition_id,
                success: false,
                match_offset: local_leo,
            };
        }

        AppendEntriesResponse {
            term: self.current_term,
            follower_id: self.node_id,
            topic: req.topic.clone(),
            partition_id: req.partition_id,
            success: true,
            match_offset: local_leo,
        }
    }

    /// Transitions this node to a Candidate, incrementing the term and voting for self.
    pub fn become_candidate(&mut self) -> VoteRequest {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.node_id);
        self.leader_id = None;
        self.last_heartbeat = Instant::now();

        VoteRequest {
            term: self.current_term,
            candidate_id: self.node_id,
            last_log_term: self.last_log_term,
            last_log_offset: self.last_log_offset,
        }
    }

    /// Transitions this node to the active Leader for the current term.
    pub fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.node_id);
        self.last_heartbeat = Instant::now();
    }
}
