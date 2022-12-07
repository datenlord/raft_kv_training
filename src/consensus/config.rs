use crate::{RaftError, INVALID_ID};

/// config definition
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Config {
    /// id is the identity of the local raft. id cannot be 0.
    pub id: u64,

    /// `election_tick` is the number of Node.Tick invocations that must pass between
    /// elections. That is, if a follower does not receive any message from the
    /// leader of current term before `election_tick` has elapsed, it will become
    /// candidate and start an election. `election_tick` must be greater than
    /// `heartbeat_tick`. We suggest `election_tick` = 10 * `heartbeat_tick` to avoid
    /// unnecessary leader switching.
    pub election_tick: usize,

    /// `heartbeat_tick` is the number of Node.Tick invocations that must pass between
    /// heartbeats. That is, a leader sends heartbeat messages to maintain its
    /// leadership every `heartbeat_tick` ticks.
    pub heartbeat_tick: usize,

    /// Applied is the last applied index. It should only be set when restarting
    /// raft. raft will not return entries to the application smaller or equal to Applied.
    /// If Applied is unset when restarting, raft might return previous applied entries.
    /// This is a very application dependent configuration.
    pub applied: u64,
}

impl Default for Config {
    #[inline]
    fn default() -> Self {
        /// A constant represents the default heartbeat tick of raft config.
        const HEARTBEAT_TICK: usize = 2;
        /// A constant represents the default election tick of raft config.
        const ELECTION_TICK: usize = 20;
        Self {
            id: 0,
            election_tick: ELECTION_TICK,
            heartbeat_tick: HEARTBEAT_TICK,
            applied: 0,
        }
    }
}

impl Config {
    /// check if the given config is valid or not
    ///
    /// # Errors
    /// It'll return `RaftError::InvalidConfig` if the id is invalid, the `heartbeat_tick` is lesser than
    /// zero or the `election_tick` is smaller than the `heartbeat_tick`
    #[inline]
    pub fn validate(&self) -> Result<(), RaftError> {
        if self.id == INVALID_ID {
            return Err(RaftError::InvalidConfig("Invalid config ID".to_owned()));
        }
        if self.heartbeat_tick == 0 {
            return Err(RaftError::InvalidConfig(
                "Heartbeat timeout must be greater than 0".to_owned(),
            ));
        }
        if self.election_tick <= self.heartbeat_tick {
            return Err(RaftError::InvalidConfig(
                "Election timeout must be greater than heartbeat timeout".to_owned(),
            ));
        }
        Ok(())
    }
}
