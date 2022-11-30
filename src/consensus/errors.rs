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
}

impl PartialEq for RaftError {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (&RaftError::Store(ref e1), &RaftError::Store(ref e2)) => e1 == e2,
            (&RaftError::InvalidConfig(ref e1), &RaftError::InvalidConfig(ref e2)) => e1 == e2,
            _ => false,
        }
    }
}

/// An error with the storage
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum StorageError {
    /// Log Unavailable
    #[error("log unavailable: last index: {0}, target index: {1}")]
    Unavailable(u64, u64),

    /// Empty Entries
    #[error("empty entries")]
    EmptyEntries(),

    /// Some other errors occurred
    #[error("unknown error {0}")]
    Others(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl PartialEq for StorageError {
    #[allow(clippy::match_same_arms)]
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (&StorageError::Unavailable(i1, i2), &StorageError::Unavailable(i3, i4)) => {
                i1 == i3 && i2 == i4
            }
            (&StorageError::EmptyEntries(), &StorageError::EmptyEntries()) => true,
            _ => false,
        }
    }
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
        assert_ne!(
            StorageError::Others(Box::new(StorageError::EmptyEntries())),
            StorageError::Unavailable(1, 2)
        );
    }
}
