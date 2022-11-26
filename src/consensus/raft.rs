#![allow(dead_code)]
use super::errors::RaftError;
use rand::Rng;
use std::collections::HashMap;
use std::fmt::Display;

/// Invalid id
const NONE: u64 = 0;

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

/// config definition
struct Config {
    /// id is the identity of the local raft. id cannot be 0.
    id: u64,

    /// peers contains the ids of all nodes (including self) in the raft cluster. It
    /// should only be set when starting a new raft cluster. Restarting raft from
    /// previous configuration will panic if peers is set. peer is private and only
    /// used for testing right now.
    peers: Vec<u64>,

    /// `election_timeout` is the number of Node.Tick invocations that must pass between
    /// elections. That is, if a follower does not receive any message from the
    /// leader of current term before `election_timeout` has elapsed, it will become
    /// candidate and start an election. `election_timeout` must be greater than
    /// `heartbeat_timeout`. We suggest `election_timeout` = 10 * `heartbeat_timeout` to avoid
    /// unnecessary leader switching.
    election_timeout: u32,

    /// `heartbeat_timeout` is the number of Node.Tick invocations that must pass between
    /// heartbeats. That is, a leader sends heartbeat messages to maintain its
    /// leadership every `heartbeat_timeout` ticks.
    heartbeat_timeout: u32,
}

impl Config {
    /// generate a new raft config
    fn new(id: u64, peers: Vec<u64>, election: u32, heartbeat: u32) -> Result<Self, RaftError> {
        Self::validate(id, election, heartbeat)?;
        Ok(Self {
            id,
            peers,
            election_timeout: election,
            heartbeat_timeout: heartbeat,
        })
    }

    /// check if the given config is valid or not
    fn validate(id: u64, election: u32, heartbeat: u32) -> Result<(), RaftError> {
        if id == NONE {
            return Err(RaftError::InvalidConfID);
        }
        if heartbeat == 0 {
            return Err(RaftError::InvalidHeartbeatTimeout);
        }
        if election <= heartbeat {
            return Err(RaftError::InvalidElectionTimeout);
        }
        Ok(())
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
    heartbeat_timeout: u32,

    /// baseline of election interval
    election_timeout: u32,

    /// randomize leader election timeout to avoid election livelock
    random_election_timeout: u32,
    /// number of ticks since it reached last heartbeatTimeout.
    /// only leader keeps heartbeatElapsed.
    heartbeat_elapsed: u32,
    /// number of ticks since it reached last electionTimeout
    election_elapsed: u32,
}

impl Raft {
    /// generate a new Raft instance
    fn new(config: &Config) -> Self {
        let mut rng = rand::thread_rng();
        let mut random_election = config.election_timeout;
        if let Some(random_timeout) =
            random_election.checked_add(rng.gen_range::<u32, _>(0..random_election))
        {
            random_election = random_timeout;
        }
        Self {
            id: config.id,
            votes: HashMap::new(),
            heartbeat_timeout: config.heartbeat_timeout,
            election_timeout: config.election_timeout,
            state: State::Follower,
            term: 0,
            lead: NONE,
            vote: NONE,
            heartbeat_elapsed: 0,
            election_elapsed: 0,
            random_election_timeout: random_election,
        }
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

    fn new_raft(
        id: u64,
        peers: Vec<u64>,
        election: u32,
        heartbeat: u32,
    ) -> Result<Raft, RaftError> {
        let conf = Config::new(id, peers, election, heartbeat)?;
        Ok(Raft::new(&conf))
    }

    #[test]
    fn start_as_follower_2aa() {
        let r = error_handle(new_raft(1, vec![1, 2, 3], 10, 1));
        assert_eq!(r.state, State::Follower);
    }
}
