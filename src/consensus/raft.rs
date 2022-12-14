#![allow(dead_code)]
use crate::log::RaftLog;
use crate::message::MsgData;
use crate::{config::Config, INVALID_ID};
use crate::{Entry, Message, Progress, RaftError, Storage};
use getset::{Getters, MutGetters, Setters};
use rand::Rng;
use std::collections::HashMap;
use std::fmt::Display;

/// peer state
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum State {
    /// Followers handle requests passively. The most of servers are in this state.
    Follower,
    /// Candidates are used to elect a new leader. Candidate is a temporary state
    /// between the Follower state and the Leader state.
    Candidate,
    /// Leader takes responsibility for handling all the requests from clients
    /// and log replication. A cluster can only have one normal leader at one time.
    Leader,
}

/// `VoteResult` indicates the outcome of a vote.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VoteResult {
    /// `Pending` indicates that the decision of the vote depends on future
    /// votes, i.e. neither "yes" or "no" has reached quorum yet.
    Pending,
    /// `Grant` indicates that the quorum has voted "yes".
    Grant,
    /// `Reject` indicates that the quorum has voted "no".
    Reject,
}

impl Display for VoteResult {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Display for State {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Default for State {
    #[inline]
    fn default() -> Self {
        Self::Follower
    }
}

/// raft node definition
#[derive(Debug, Getters, MutGetters, Setters)]
pub struct Raft<T: Storage> {
    /// raft id
    pub id: u64,
    /// current term
    pub term: u64,
    /// which candidate is this peer to voted for
    vote: u64,
    /// this peer's role
    pub role: State,

    /// the leader id
    leader_id: u64,

    /// votes records
    #[get = "pub"]
    votes: HashMap<u64, bool>,

    /// heartbeat interval, should send
    heartbeat_timeout: usize,

    /// baseline of election interval
    election_timeout: usize,

    /// randomize leader election timeout to avoid election livelock
    random_election_timeout: usize,
    /// number of ticks since it reached last heartbeatTimeout.
    /// only leader keeps heartbeatElapsed.
    heartbeat_elapsed: usize,
    /// number of ticks since it reached last electionTimeout
    election_elapsed: usize,

    /// The persistent log
    pub raft_log: RaftLog<T>,

    /// The list of messages
    pub msgs: Vec<Message>,

    ///
    #[getset(get_mut)]
    progresses: HashMap<u64, Progress>,
}

impl<T: Storage> Raft<T> {
    /// generate a new Raft instance
    ///
    /// # Errors
    ///
    /// return `RaftError::InvalidConfig` when the given config is invalid.
    /// see `Config::validate()` in config.rs for more information
    #[inline]
    pub fn new(config: &Config, storage: T) -> Result<Self, RaftError> {
        config.validate()?;
        let raft_state = storage.initial_state();
        let conf_state = &raft_state.conf_state;
        let peers = &conf_state.peers;
        let mut rng = rand::thread_rng();
        let mut random_election = config.election_tick;
        if let Some(random_timeout) =
            random_election.checked_add(rng.gen_range::<usize, _>(0..random_election))
        {
            random_election = random_timeout;
        }
        let mut raft = Self {
            id: config.id,
            votes: HashMap::with_capacity(peers.len()),
            heartbeat_timeout: config.heartbeat_tick,
            election_timeout: config.election_tick,
            role: State::Follower,
            term: 0,
            leader_id: INVALID_ID,
            vote: INVALID_ID,
            heartbeat_elapsed: 0,
            election_elapsed: 0,
            random_election_timeout: random_election,
            raft_log: RaftLog::new(storage),
            msgs: Vec::new(),
            progresses: HashMap::with_capacity(peers.len()),
        };
        for id in peers {
            let _ = raft.progresses.insert(*id, Progress::default());
        }
        Ok(raft)
    }

    /// convert this node to a follower
    #[inline]
    pub fn become_follower(&mut self, term: u64, leader_id: u64) {
        self.reset(term);
        self.vote = INVALID_ID;
        self.role = State::Follower;
        self.term = term;
        self.leader_id = leader_id;
    }

    /// convert this node to a candidate
    #[allow(clippy::integer_arithmetic)]
    #[inline]
    pub fn become_candidate(&mut self) {
        self.reset(self.term + 1);
        self.role = State::Candidate;
        self.vote = self.id;
    }

    /// convert this node to a leader
    #[inline]
    #[allow(clippy::integer_arithmetic)]
    pub fn become_leader(&mut self) {
        let term = self.term;
        self.reset(term);
        self.leader_id = self.id;
        self.role = State::Leader;
        let last_index = self.raft_log.buffer_last_index();
        for (&id, peer) in &mut self.progresses {
            if id == self.id {
                peer.matched = last_index + 1;
                peer.next_idx = last_index + 2;
            } else {
                peer.next_idx = last_index + 1;
            }
        }
        let _res = self.raft_log.append(&[Entry::default()]);
    }

    /// Resets the current node to a given term.
    #[allow(clippy::integer_arithmetic)]
    fn reset(&mut self, term: u64) {
        if self.term != term {
            self.term = term;
            self.vote = INVALID_ID;
        }
        self.leader_id = INVALID_ID;
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;
        self.random_election_timeout =
            rand::thread_rng().gen_range(self.election_timeout..2 * self.election_timeout);
        self.votes.clear();
        let last_index = self.raft_log.buffer_last_index();
        let committed = self.raft_log.committed;
        let persisted = self.raft_log.persisted;
        let self_id = self.id;
        for (&id, mut pr) in self.progresses_mut() {
            pr.matched = 0;
            pr.next_idx = last_index + 1;
            if id == self_id {
                pr.matched = persisted;
                pr.committed_index = committed;
            }
        }
    }

    /// Returns true to indicate that there will probably be some readiness need to be handled.
    #[inline]
    pub fn tick(&mut self) {
        self.election_elapsed += 1;
        if self.election_elapsed >= self.random_election_timeout {
            self.election_elapsed = 0;
        }
        match self.role {
            State::Follower | State::Candidate => {
                if self.election_elapsed == 0 {
                    self.tick_election();
                }
            }
            State::Leader => {
                if self.election_elapsed != 0 {
                    self.tick_heartbeat();
                }
            }
        }
    }

    /// Run by followers and candidates after `self.election_timeout`.
    ///
    /// Returns true to indicate that there will probably be some readiness need to be handled.
    #[allow(clippy::integer_arithmetic)]
    #[inline]
    fn tick_election(&mut self) {
        let m = Message::new_hup_msg(self.id, INVALID_ID);
        self.step(&m);
    }

    /// Run by a leader to send MsgBeat after `self.heartbeat_timeout`
    ///
    /// Returns true to indicate that there will probably be some readiness need to be handled.
    #[allow(clippy::integer_arithmetic)]
    #[inline]
    fn tick_heartbeat(&mut self) {
        self.heartbeat_elapsed += 1;
        if self.heartbeat_elapsed >= self.heartbeat_timeout {
            self.heartbeat_elapsed = 0;
            self.step(&Message::new_beat_msg(INVALID_ID, INVALID_ID));
        }
    }

    /// Read messages out of the raft
    #[inline]
    pub fn read_messages(&mut self) -> Vec<Message> {
        self.msgs.drain(..).collect()
    }

    /// Steps the raft along via a message. This should be called everytime your raft receives a
    /// message from a peer.
    #[inline]
    pub fn step(&mut self, msg: &Message) {
        match msg.msg_data {
            Some(MsgData::Hup(_m)) => self.hup(),
            Some(MsgData::Beat(_m)) => self.bcast_heartbeat(),
            _ => unreachable!(),
        }
    }

    /// hup message's handler
    #[allow(clippy::integer_arithmetic)]
    fn hup(&mut self) {
        if self.role == State::Leader {
            // output some log
            return;
        }
        self.campaign();
    }

    /// Campaign to attempt to become a leader.
    #[inline]
    pub fn campaign(&mut self) {
        self.become_candidate();
        let last_log_term = self.raft_log.last_term();
        let last_log_index = self.raft_log.buffer_last_index();
        let term = self.term;
        let self_id = self.id;

        if VoteResult::Grant == self.poll(self_id, true) {
            return;
        }
        for &id in self.progresses.keys() {
            if id == self_id {
                continue;
            }
            let vote_req_msg =
                Message::new_request_vote_msg(self_id, id, term, last_log_term, last_log_index);
            self.msgs.push(vote_req_msg);
        }
    }

    /// Voting statistics
    fn poll(&mut self, id: u64, vote: bool) -> VoteResult {
        self.record_vote(id, vote);
        let res = self.tally_votes();
        match res {
            VoteResult::Grant => {
                self.become_leader();
            }
            VoteResult::Reject => {
                self.become_follower(self.term, INVALID_ID);
            }
            VoteResult::Pending => (),
        }
        res
    }

    /// Records that the node with the given id voted for this Raft
    /// instance if v == true (and declined it otherwise).
    #[inline]
    pub fn record_vote(&mut self, id: u64, vote: bool) {
        let _res = self.votes.entry(id).or_insert(vote);
    }

    /// `tally_votes` returns the result of the election quorum
    #[inline]
    #[allow(clippy::integer_arithmetic)]
    pub fn tally_votes(&self) -> VoteResult {
        let majority = (self.progresses.len() / 2) + 1;
        let granted = self.votes.values().filter(|&&v| v).count();
        let rejected = self.votes.len() - granted;
        let missing = self.votes.capacity() - granted - rejected;
        if granted >= majority {
            VoteResult::Grant
        } else if granted + missing >= majority {
            VoteResult::Pending
        } else {
            VoteResult::Reject
        }
    }

    /// Appends a slice of entries to the log.
    /// The entries are updated to match the current index and term.
    /// Only called by leader currently
    #[must_use]
    pub fn append_entry(&mut self, es: &mut [Entry]) -> bool {
        let li = self.raft_log.buffer_last_index();
        for (i, e) in es.iter_mut().enumerate() {
            e.term = self.term;
            e.index = li + 1 + i as u64;
        }
        let _res = self.raft_log.append(es);
        true
    }

    /// Broadcast heartbeat to all the followers
    fn bcast_heartbeat(&mut self) {
        let self_id = self.id;
        self.progresses
            .clone()
            .keys()
            .filter(|&id| *id != self_id)
            .for_each(|id| self.send_heartbeat(*id));
    }

    /// Send a heartbeat to the given peer
    fn send_heartbeat(&mut self, to: u64) {
        let msg = Message::new_heartbeat_msg(self.id, to, self.term);
        self.send(msg);
    }

    /// send the given message to the mailbox
    fn send(&mut self, m: Message) {
        self.msgs.push(m);
    }
}
