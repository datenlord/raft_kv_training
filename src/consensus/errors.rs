use thiserror::Error;

/// Error Definition in raft
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum RaftError {
    /// Invalid Config
    #[error("{0}")]
    InvalidConfig(String),
    /// Storage relevant errors
    #[error("{0}")]
    Store(#[from] StorageError),
    /// RaftLog relevant errors
    #[error("{0}")]
    Log(#[from] LogError),
    /// Some other errors occurred
    #[error("unknown error {0}")]
    Others(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl PartialEq for RaftError {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (&RaftError::Store(ref e1), &RaftError::Store(ref e2)) => e1 == e2,
            (&RaftError::InvalidConfig(ref e1), &RaftError::InvalidConfig(ref e2)) => e1 == e2,
            (&RaftError::Log(ref e1), &RaftError::Log(ref e2)) => e1 == e2,
            _ => false,
        }
    }
}

/// An error with the storage
#[non_exhaustive]
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum StorageError {
    /// Log Unavailable
    #[error("logs from {0} to {1} are unavailable")]
    Unavailable(u64, u64),

    /// Empty Entries
    #[error("empty entries")]
    EmptyEntries(),
}

/// An error with the `RaftLog`
#[non_exhaustive]
#[derive(Debug, Error, PartialEq, Eq, Clone, Copy)]
pub enum LogError {
    /// Log index is out of range
    #[error("index must be less than the last log index: index = {0}, last_index = {1}")]
    IndexOutOfBounds(u64, u64),
    /// Empty unstable log
    #[error("Unstable log entries are empty")]
    EmptyUnstableLog(),
    /// Truncated stable log entriees is not allowed
    #[error("Cannot truncate stable log entries [{0}, {1})")]
    TruncatedStableLog(u64, u64),
    /// Unconsistent log entries
    #[error(
        "The last entry of unstable_logs has different index {0} and term {1}, expect {2} {3} "
    )]
    Unconsistent(u64, u64, u64, u64),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_error_equal() {
        assert_eq!(
            RaftError::InvalidConfig("Invalid config ID".to_owned()),
            RaftError::InvalidConfig("Invalid config ID".to_owned())
        );

        assert_ne!(
            RaftError::InvalidConfig("Invalid config ID".to_owned()),
            RaftError::InvalidConfig("Heartbeat timeout must be greater than 0".to_owned())
        );
        assert_ne!(
            RaftError::Store(StorageError::EmptyEntries()),
            RaftError::Store(StorageError::Unavailable(1, 2))
        );
        assert_eq!(
            RaftError::Store(StorageError::Unavailable(1, 2)),
            RaftError::Store(StorageError::Unavailable(1, 2))
        );
        assert_ne!(
            RaftError::Others(Box::new(StorageError::EmptyEntries())),
            RaftError::InvalidConfig("hello".to_owned())
        );
    }

    #[test]
    fn test_storage_error_equal() {
        assert_eq!(
            StorageError::Unavailable(1, 2),
            StorageError::Unavailable(1, 2)
        );

        assert_ne!(
            StorageError::Unavailable(1, 2),
            StorageError::Unavailable(3, 4)
        );

        assert_ne!(
            StorageError::Unavailable(1, 2),
            StorageError::EmptyEntries()
        );
    }

    #[test]
    fn test_log_error_equal() {
        assert_eq!(
            LogError::IndexOutOfBounds(1, 2),
            LogError::IndexOutOfBounds(1, 2),
        );

        assert_ne!(
            LogError::IndexOutOfBounds(1, 2),
            LogError::IndexOutOfBounds(3, 4)
        );

        assert_eq!(
            LogError::TruncatedStableLog(1, 2),
            LogError::TruncatedStableLog(1, 2)
        );

        assert_ne!(
            LogError::TruncatedStableLog(1, 2),
            LogError::TruncatedStableLog(3, 4)
        );
    }
}
