use thiserror::Error;

/// Error Definition in raft
#[non_exhaustive]
#[derive(Debug, Error, Copy, Clone)]
pub enum RaftError {
    /// Invalid Config id
    #[error("Cannot use none as id")]
    InvalidConfID,
    /// Invalid heartbeat timeout
    #[error("heartbeat timeout must be greater than 0")]
    InvalidHeartbeatTimeout,
    /// Invalid election timeout
    #[error("election tick must be greater than heartbeat tick")]
    InvalidElectionTimeout,
}
