#![allow(dead_code)]
use crate::log::RaftLog;
use crate::message::MsgData;
use crate::{config::Config, INVALID_ID};
use crate::{
    Entry, HardState, LogError, Message, MsgAppend, MsgAppendResponse, MsgHeartbeat,
    MsgHeartbeatResponse, MsgPropose, MsgRequestVote, MsgRequestVoteResponse, Progress, RaftError,
    Storage,
};
use getset::{Getters, MutGetters, Setters};
use rand::Rng;
use std::cmp::Ordering;
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
    #[get = "pub"]
    id: u64,
    /// current term
    pub term: u64,
    /// which candidate is this peer to voted for
    pub vote: u64,
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

    /// The log progress of all nodes in the cluster
    #[get_mut = "pub"]
    progresses: HashMap<u64, Progress>,

    /// success append message counter
    append_counter: usize,
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
            append_counter: 0,
        };
        for id in peers {
            let _ = raft.progresses.insert(
                *id,
                Progress::new(raft.raft_log.buffer_last_index(), raft.raft_log.last_term()),
            );
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
    }

    /// Run by each peer in a raft cluster to advance the logical clock
    #[inline]
    pub fn tick(&mut self) {
        match self.role {
            State::Follower | State::Candidate => self.tick_election(),
            State::Leader => self.tick_heartbeat(),
        }
    }

    /// Run by followers and candidates after `self.election_timeout`.
    #[allow(clippy::integer_arithmetic)]
    #[inline]
    fn tick_election(&mut self) {
        self.election_elapsed += 1;
        if self.election_elapsed >= self.random_election_timeout {
            self.election_elapsed = 0;
            let m = Message::new_hup_msg(self.id, INVALID_ID);
            self.step(&m);
        }
    }

    /// Run by a leader to send `MsgBeat` after `self.heartbeat_timeout`
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
        match self.role {
            State::Leader => self.step_leader(msg),
            State::Follower => self.step_follwer(msg),
            State::Candidate => self.step_candidate(msg),
        }
    }

    /// The concreted step implementation for a leader
    #[inline]
    fn step_leader(&mut self, msg: &Message) {
        match msg.msg_data {
            Some(MsgData::Hup(_m)) => (),
            Some(MsgData::Beat(_m)) => self.bcast_heartbeat(),
            Some(MsgData::Heartbeat(m)) => self.handle_heartbeat_msg(&m),
            Some(MsgData::HeartbeatResponse(m)) => self.handle_heartbeat_reply(&m),
            Some(MsgData::RequestVote(m)) => self.handle_request_vote_msg(&m),
            Some(MsgData::RequestVoteResponse(m)) => self.handle_request_vote_reply(&m),
            Some(MsgData::Append(ref m)) => self.handle_append_entries_msg(m),
            Some(MsgData::AppendResponse(ref m)) => self.handle_append_entries_reply(m),
            Some(MsgData::Propose(ref m)) => self.handle_propose_msg(m),
            _ => unreachable!(),
        }
    }

    /// The concreted step implementation for a follower
    #[inline]
    fn step_follwer(&mut self, msg: &Message) {
        match msg.msg_data {
            Some(MsgData::Hup(_m)) => self.handle_hup_msg(),
            Some(MsgData::Heartbeat(ref m)) => self.handle_heartbeat_msg(m),
            Some(MsgData::RequestVote(ref m)) => self.handle_request_vote_msg(m),
            Some(MsgData::RequestVoteResponse(ref m)) => self.handle_request_vote_reply(m),
            Some(MsgData::Append(ref m)) => self.handle_append_entries_msg(m),
            Some(MsgData::Propose(ref m)) => self.handle_propose_msg(m),
            _ => unreachable!(),
        }
    }

    /// The concreted step implementation for a candidate
    #[inline]
    fn step_candidate(&mut self, msg: &Message) {
        match msg.msg_data {
            Some(MsgData::Hup(_m)) => self.handle_hup_msg(),
            Some(MsgData::Heartbeat(m)) => self.handle_heartbeat_msg(&m),
            Some(MsgData::RequestVote(m)) => self.handle_request_vote_msg(&m),
            Some(MsgData::RequestVoteResponse(m)) => self.handle_request_vote_reply(&m),
            Some(MsgData::Append(ref m)) => self.handle_append_entries_msg(m),
            Some(MsgData::Propose(ref _m)) => (),
            _ => unreachable!(),
        }
    }

    /// hup message's handler
    #[allow(clippy::integer_arithmetic)]
    fn handle_hup_msg(&mut self) {
        self.become_candidate();
        let last_log_term = self.raft_log.last_term();
        let last_log_index = self.raft_log.buffer_last_index();
        let term = self.term;
        let self_id = self.id;

        self.poll(self_id, true);
        for &id in self.progresses.clone().keys() {
            if id == self_id {
                continue;
            }
            let vote_req_msg =
                Message::new_request_vote_msg(self_id, id, term, last_log_term, last_log_index);
            self.send(vote_req_msg);
        }
    }

    /// heartbeat message's handler
    fn handle_heartbeat_msg(&mut self, msg: &MsgHeartbeat) {
        let reject = if self.term > msg.term {
            true
        } else {
            self.become_follower(msg.term, msg.from);
            false
        };
        let heartbeat_reply = Message::new_heartbeat_resp_msg(self.id, msg.from, self.term, reject);
        self.send(heartbeat_reply);
    }

    /// heartbeat message's handler
    fn handle_heartbeat_reply(&mut self, msg: &MsgHeartbeatResponse) {
        if msg.reject {
            self.become_follower(msg.term, INVALID_ID);
        }
    }

    /// raft log integrit check
    /// return true indicates that we should reject the vote request
    fn log_integrity_check(&self, log_index: u64, log_term: u64) -> bool {
        let last_log_term = self.raft_log.last_term();
        last_log_term > log_term
            || (last_log_term == log_term && self.raft_log.buffer_last_index() > log_index)
    }

    /// request vote message's handler
    fn handle_request_vote_msg(&mut self, msg: &MsgRequestVote) {
        let reject = match self.term.cmp(&msg.term) {
            Ordering::Greater => true,
            Ordering::Equal => {
                if (self.vote != INVALID_ID && self.vote != msg.from)
                    || self.log_integrity_check(msg.last_log_index, msg.last_log_term)
                {
                    true
                } else {
                    self.vote = msg.from;
                    false
                }
            }
            Ordering::Less => {
                self.become_follower(msg.term, INVALID_ID);
                if self.log_integrity_check(msg.last_log_index, msg.last_log_term) {
                    true
                } else {
                    self.vote = msg.from;
                    false
                }
            }
        };

        let request_vote_reply = Message::new_request_vote_resp_msg(
            self.id,
            msg.from,
            self.term,
            reject,
            self.raft_log.buffer_last_index(),
            self.raft_log.last_term(),
        );
        self.send(request_vote_reply);
    }

    /// request vote reply message's handler
    fn handle_request_vote_reply(&mut self, msg: &MsgRequestVoteResponse) {
        if msg.term > self.term {
            self.become_follower(msg.term, INVALID_ID);
        } else if self.role == State::Candidate {
            if let Some(progress) = self.progresses.get_mut(&msg.from) {
                progress.log_index = msg.last_log_index;
                progress.log_term = msg.last_log_term;
            }
            self.poll(msg.from, !msg.reject);
        } else {
            // make clippy happy
        }
    }

    /// propose message's handler
    #[allow(clippy::integer_arithmetic, clippy::expect_used)]
    fn handle_propose_msg(&mut self, msg: &MsgPropose) {
        if self.role == State::Follower {
            if self.leader_id != INVALID_ID {
                let new_propose_msg =
                    Message::new_propose_msg(msg.from, self.leader_id, msg.entries.clone());
                self.send(new_propose_msg);
            }
        } else {
            // If the follower has an uncommitted log tail, we would end up
            // probing one by one until we hit the common prefix.
            //
            // For example, if the leader has:
            //
            //   idx        1 2 3 4 5 6 7 8 9
            //              -----------------
            //   term (L)   1 3 3 3 5 5 5 5 5
            //   term (F)   1 1 1 1 2 2
            //
            // Then, after sending an append anchored at (idx=9,term=5) we
            // would receive a RejectHint of 6 and LogTerm of 2. Without the
            // code below, we would try an append at index 6, which would
            // fail again.
            //
            // However, looking only at what the leader knows about its own
            // log and the rejection hint, it is clear that a probe at index
            // 6, 5, 4, 3, and 2 must fail as well:
            //
            // For all of these indexes, the leader's log term is larger than
            // the rejection's log term. If a probe at one of these indexes
            // succeeded, its log term at that index would match the leader's,
            // i.e. 3 or 5 in this example. But the follower already told the
            // leader that it is still at term 2 at index 9, and since the
            // log term only ever goes up (within a log), this is a contradiction.
            //
            // At index 1, however, the leader can draw no such conclusion,
            // as its term 1 is not larger than the term 2 from the
            // follower's rejection. We thus probe at 1, which will succeed
            // in this example. In general, with this approach we probe at
            // most once per term found in the leader's log.
            //
            // There is a similar mechanism on the follower (implemented in
            // handleAppendEntries via a call to findConflictByTerm) that is
            // useful if the follower has a large divergent uncommitted log
            // tail[1], as in this example:
            //
            //   idx        1 2 3 4 5 6 7 8 9
            //              -----------------
            //   term (L)   1 3 3 3 3 3 3 3 7
            //   term (F)   1 3 3 4 4 5 5 5 6
            //
            // Naively, the leader would probe at idx=9, receive a rejection
            // revealing the log term of 6 at the follower. Since the leader's
            // term at the previous index is already smaller than 6, the leader-
            // side optimization discussed above is ineffective. The leader thus
            // probes at index 8 and, naively, receives a rejection for the same
            // index and log term 5. Again, the leader optimization does not improve
            // over linear probing as term 5 is above the leader's term 3 for that
            // and many preceding indexes; the leader would have to probe linearly
            // until it would finally hit index 3, where the probe would succeed.
            //
            // Instead, we apply a similar optimization on the follower. When the
            // follower receives the probe at index 8 (log term 3), it concludes
            // that all of the leader's log preceding that index has log terms of
            // 3 or below. The largest index in the follower's log with a log term
            // of 3 or below is index 3. The follower will thus return a rejection
            // for index=3, log term=3 instead. The leader's next probe will then
            // succeed at that index.
            //
            // [1]: more precisely, if the log terms in the large uncommitted
            // tail on the follower are larger than the leader's. At first,
            // it may seem unintuitive that a follower could even have such
            // a large tail, but it can happen:
            //
            // 1. Leader appends (but does not commit) entries 2 and 3, crashes.
            //   idx        1 2 3 4 5 6 7 8 9
            //              -----------------
            //   term (L)   1 2 2     [crashes]
            //   term (F)   1
            //   term (F)   1
            //
            // 2. a follower becomes leader and appends entries at term 3.
            //              -----------------
            //   term (x)   1 2 2     [down]
            //   term (F)   1 3 3 3 3
            //   term (F)   1
            //
            // 3. term 3 leader goes down, term 2 leader returns as term 4
            //    leader. It commits the log & entries at term 4.
            //
            //              -----------------
            //   term (L)   1 2 2 2
            //   term (x)   1 3 3 3 3 [down]
            //   term (F)   1
            //              -----------------
            //   term (L)   1 2 2 2 4 4 4
            //   term (F)   1 3 3 3 3 [gets probed]
            //   term (F)   1 2 2 2 4 4 4
            //
            // 4. the leader will now probe the returning follower at index
            //    7, the rejection points it at the end of the follower's log
            //    which is at a higher log term than the actually committed
            //    log.
            let (commit_index, _) = self.raft_log.commit_info();
            let mut ents = msg.entries.clone();
            if self.append_entry(&mut ents[..]) {
                self.raft_log.persist_log_buffer();
                self.append_counter += 1;
                for (id, progress) in self.progresses.clone() {
                    if id == self.id {
                        continue;
                    }
                    let anchor_idx = progress.log_index;
                    let anchor_term = progress.log_term;
                    if let Ok(log_term) = self.raft_log.term(anchor_idx) {
                        let append_msg = match anchor_term.cmp(&log_term) {
                            Ordering::Equal => {
                                // Because of the log integrity check, so the `anchor_idx` + 1 is always less or equal than `self.raft_log.buffer_last_index()`,
                                // the `entries` won't return any error here.
                                let entries = self
                                    .raft_log
                                    .entries(anchor_idx + 1, self.raft_log.buffer_last_index())
                                    .expect("Undefinded behaviour");
                                Message::new_append_msg(
                                    self.id,
                                    id,
                                    self.term,
                                    commit_index,
                                    anchor_idx,
                                    anchor_term,
                                    entries,
                                )
                            }
                            Ordering::Less | Ordering::Greater => {
                                let (index, term) = self
                                    .raft_log
                                    .get_next_unconfilict_index(anchor_idx, anchor_term);

                                let entries = self
                                    .raft_log
                                    .entries(index + 1, self.raft_log.buffer_last_index())
                                    .expect("Undefined behaviour");
                                Message::new_append_msg(
                                    self.id,
                                    id,
                                    self.term,
                                    commit_index,
                                    index,
                                    term,
                                    entries,
                                )
                            }
                        };
                        self.send(append_msg);
                    }
                }
            }
        }
    }

    /// do append and then commit as a follower
    fn do_append_as_a_follower(&mut self, msg: &MsgAppend) -> bool {
        self.become_follower(msg.term, msg.from);

        if self.raft_log.append(&msg.entries[..]).is_ok() {
            self.raft_log.commit_to(msg.leader_commit);
            false
        } else {
            true
        }
    }

    /// log consistency check
    fn log_consistency_check(&self, prev_log_index: u64, prev_log_term: u64) -> bool {
        !self.raft_log.match_term(prev_log_index, prev_log_term)
    }

    /// append entries message's handler
    fn handle_append_entries_msg(&mut self, msg: &MsgAppend) {
        let reject = match self.term.cmp(&msg.term) {
            Ordering::Greater => true,
            Ordering::Equal => {
                if self.role == State::Leader {
                    unreachable!("Never allow two leader in the same term in a raft cluster!!")
                }
                self.log_consistency_check(msg.prev_log_index, msg.prev_log_term)
                    || self.do_append_as_a_follower(msg)
            }
            Ordering::Less => {
                self.log_consistency_check(msg.prev_log_index, msg.prev_log_term)
                    || self.do_append_as_a_follower(msg)
            }
        };

        let (prev_log_index, prev_log_term) = if reject {
            if self
                .raft_log
                .match_term(msg.prev_log_index, msg.prev_log_term)
            {
                (msg.prev_log_index, msg.prev_log_term)
            } else {
                self.raft_log
                    .get_next_unconfilict_index(msg.prev_log_index, msg.prev_log_term)
            }
        } else {
            (self.raft_log.buffer_last_index(), self.raft_log.last_term())
        };

        let append_reply = Message::new_append_resp_msg(
            self.id,
            msg.from,
            self.term,
            reject,
            prev_log_index,
            prev_log_term,
        );
        self.send(append_reply);
    }

    /// count the successful append reply number
    #[allow(clippy::integer_arithmetic)]
    #[inline]
    fn majority_append_success(&self) -> bool {
        self.append_counter > (self.progresses.len() / 2)
    }

    /// append message reply message's handler
    #[allow(clippy::integer_arithmetic, clippy::expect_used)]
    #[inline]
    fn handle_append_entries_reply(&mut self, msg: &MsgAppendResponse) {
        if self.majority_append_success() {
            self.append_counter = 0;
            return;
        }
        if msg.reject {
            let (commit_idx, _) = self.raft_log.commit_info();
            let (index, term) = self
                .raft_log
                .get_next_unconfilict_index(msg.last_log_index, msg.last_log_term);
            // Because index is always valid, `entries` should never return any index error
            // It's ok to expect here.
            let entries = self
                .raft_log
                .entries(index, self.raft_log.buffer_last_index())
                .expect("Undefined behaviour");
            let append_msg = Message::new_append_msg(
                self.id, msg.from, self.term, commit_idx, index, term, entries,
            );
            self.send(append_msg);
        } else {
            if let Some(pr) = self.progresses.get_mut(&msg.from) {
                pr.log_index = msg.last_log_index;
                pr.log_term = msg.last_log_term;
            }
            self.append_counter += 1;
        }
    }

    /// Load the given `hardstate` into self
    ///
    /// # Errors
    ///
    /// Return `TruncateCommittedLog` if `hs.committed` is less than `self.raft_log.committed`.
    #[allow(clippy::integer_arithmetic)]
    #[inline]
    pub fn load_state(&mut self, hs: &HardState) -> Result<(), RaftError> {
        if hs.committed < self.raft_log.committed {
            Err(RaftError::Log(LogError::TruncateCommittedLog(
                hs.committed,
                self.raft_log.committed,
            )))
        } else if hs.committed > self.raft_log.buffer_last_index() {
            Err(RaftError::Log(LogError::Unavailable(
                self.raft_log.buffer_last_index() + 1,
                hs.committed,
            )))
        } else {
            self.raft_log.committed = hs.committed;
            self.term = hs.term;
            self.vote = hs.voted_for;
            Ok(())
        }
    }

    /// Voting statistics
    fn poll(&mut self, id: u64, vote: bool) {
        self.record_vote(id, vote);
        match self.tally_votes() {
            VoteResult::Grant => {
                self.become_leader();
            }
            VoteResult::Reject => {
                self.become_follower(self.term, INVALID_ID);
            }
            VoteResult::Pending => (),
        }
    }

    /// Records that the node with the given id voted for this Raft
    /// instance if v == true (and declined it otherwise).
    #[inline]
    pub fn record_vote(&mut self, id: u64, vote: bool) {
        let _res = self
            .votes
            .entry(id)
            .and_modify(|val| *val = vote)
            .or_insert(vote);
    }

    /// `tally_votes` returns the result of the election quorum
    #[inline]
    #[allow(clippy::integer_arithmetic)]
    pub fn tally_votes(&self) -> VoteResult {
        let majority = (self.progresses.len() / 2) + 1;
        let granted = self.votes.values().filter(|&&v| v).count();
        let rejected = self.votes.len() - granted;
        let missing = self.progresses.len() - granted - rejected;
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
    #[allow(clippy::integer_arithmetic)]
    #[must_use]
    #[inline]
    pub fn append_entry(&mut self, es: &mut [Entry]) -> bool {
        let li = self.raft_log.buffer_last_index();
        let mut cnt = 1u64;
        for e in es.iter_mut() {
            e.term = self.term;
            e.index = li + cnt;
            cnt += 1;
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
