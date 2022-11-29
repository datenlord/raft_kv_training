use thiserror::Error;

/// Error Definition in raft
#[non_exhaustive]
#[derive(Debug, Error, Clone)]
pub enum RaftError {
    /// Invalid Config
    #[error("{0}")]
    InvalidConfig(String),
}
