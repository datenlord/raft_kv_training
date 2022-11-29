#![allow(dead_code)]
use crate::RaftError;
use crate::{config::Config, INVALID_ID};
use rand::Rng;
use std::collections::HashMap;
use std::fmt::Display;

/// peer state
#[derive(Debug, PartialEq, Eq)]
enum State {
    /// Followers handle requests passively. The most of servers are in this state.
    Follower,
    /// Candidates are used to elect a new leader. Candidate is a temporary state
    /// between the Follower state and the Leader state.
    Candidate,
    /// Leader takes responsibility for handling all the requests from clients
    /// and log replication. A cluster can only have one normal leader at one time.
    Leader,
}

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Default for State {
    fn default() -> Self {
        Self::Follower
    }
}

/// raft node definition
struct Raft {
    /// raft id
    id: u64,
    /// current term
    term: u64,
    /// which candidate is this peer to voted for
    vote: u64,
    /// this peer's role
    state: State,

    /// the leader id
    lead: u64,

    /// votes records
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
}

impl Raft {
    /// generate a new Raft instance
    fn new(config: &Config) -> Result<Self, RaftError> {
        config.validate()?;
        let mut rng = rand::thread_rng();
        let mut random_election = config.election_tick;
        if let Some(random_timeout) =
            random_election.checked_add(rng.gen_range::<usize, _>(0..random_election))
        {
            random_election = random_timeout;
        }
        Ok(Self {
            id: config.id,
            votes: HashMap::new(),
            heartbeat_timeout: config.heartbeat_tick,
            election_timeout: config.election_tick,
            state: State::Follower,
            term: 0,
            lead: INVALID_ID,
            vote: INVALID_ID,
            heartbeat_elapsed: 0,
            election_elapsed: 0,
            random_election_timeout: random_election,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Debug;

    fn error_handle<T, E>(res: Result<T, E>) -> T
    where
        E: Debug,
    {
        match res {
            Ok(t) => t,
            Err(e) => {
                unreachable!("{:?}", e)
            }
        }
    }

    fn new_test_config(id: u64, election_tick: usize, heartbeat_tick: usize) -> Config {
        Config {
            id,
            election_tick,
            heartbeat_tick,
            ..Default::default()
        }
    }

    #[test]
    fn start_as_follower_2aa() {
        let config = new_test_config(1, 10, 1);
        let r = error_handle(Raft::new(&config));
        assert_eq!(r.state, State::Follower);
    }
}
