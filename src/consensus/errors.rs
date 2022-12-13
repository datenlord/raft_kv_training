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

/// An error with the storage
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum StorageError {
    /// Log Unavailable
    #[error("log[{0}] is unavailable")]
    LogEntryUnavailable(u64),
    /// Log Unavailable
    #[error("logs from {0} to {1} are unavailable")]
    Unavailable(u64, u64),
    /// Invalid stable log index
    #[error("stable log index is invalid: {0}")]
    InvalidIndex(u64),
    /// Empty Entries
    #[error("empty entries")]
    EmptyEntries(),
    /// Other errors, including IO Error or some other expected errors
    #[error("storage unknown error: {0}")]
    Others(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// An error with the `RaftLog`
#[non_exhaustive]
#[derive(Debug, Error, Clone, Copy)]
pub enum LogError {
    /// Invalid unstable log Index
    #[error("unstable log index is invalid: {0}")]
    InvalidIndex(u64),
    /// Log Unavailable
    #[error("logs from {0} to {1} are unavailable")]
    Unavailable(u64, u64),
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
    /// Log Unavailable
    #[error("log[{0}] is unavailable")]
    LogEntryUnavailable(u64),
}

/// A macro
#[macro_export]
macro_rules! result_match {
    ($lhs: expr, $rhs: expr) => {
        match ($lhs, $rhs) {
            (Ok(l), Ok(r)) => l == r,
            (Err(l), Err(r)) => match (l, r) {
                (
                    $crate::errors::RaftError::InvalidConfig(l),
                    $crate::errors::RaftError::InvalidConfig(r),
                ) => l == r,
                ($crate::errors::RaftError::Store(l), $crate::errors::RaftError::Store(r)) => {
                    match (l, r) {
                        (
                            $crate::errors::StorageError::InvalidIndex(l),
                            $crate::errors::StorageError::InvalidIndex(r),
                        )
                        | (
                            $crate::errors::StorageError::LogEntryUnavailable(l),
                            $crate::errors::StorageError::LogEntryUnavailable(r),
                        ) => l == r,
                        (
                            $crate::errors::StorageError::EmptyEntries(),
                            $crate::errors::StorageError::EmptyEntries(),
                        ) => true,
                        (
                            $crate::errors::StorageError::Unavailable(l1, l2),
                            $crate::errors::StorageError::Unavailable(r1, r2),
                        ) => l1 == r1 && l2 == r2,
                        _ => false,
                    }
                }
                ($crate::errors::RaftError::Log(l), $crate::errors::RaftError::Log(r)) => {
                    match (l, r) {
                        (
                            $crate::errors::LogError::EmptyUnstableLog(),
                            $crate::errors::LogError::EmptyUnstableLog(),
                        ) => true,
                        (
                            $crate::errors::LogError::InvalidIndex(l),
                            $crate::errors::LogError::InvalidIndex(r),
                        )
                        | (
                            $crate::errors::LogError::LogEntryUnavailable(l),
                            $crate::errors::LogError::LogEntryUnavailable(r),
                        ) => l == r,
                        (
                            $crate::errors::LogError::Unavailable(l1, l2),
                            $crate::errors::LogError::Unavailable(r1, r2),
                        ) => l1 == r1 && l2 == r2,
                        (
                            $crate::errors::LogError::Unconsistent(l1, l2, l3, l4),
                            $crate::errors::LogError::Unconsistent(r1, r2, r3, r4),
                        ) => l1 == r1 && l2 == r2 && l3 == r3 && l4 == r4,
                        (
                            $crate::errors::LogError::TruncatedStableLog(l1, l2),
                            $crate::errors::LogError::TruncatedStableLog(r1, r2),
                        ) => l1 == r1 && l2 == r2,
                        _ => false,
                    }
                }
                _ => false,
            },
            _ => false,
        }
    };
}

/// `result_eq` asserts that two given result are equal
#[macro_export]
macro_rules! result_eq {
    ($lhs: expr, $rhs: expr) => {
        std::assert!(
            $crate::result_match!($lhs, $rhs)
        )
    };
    ($lhs: expr, $rhs: expr, $($arg:tt)+) => {
        std::assert!($crate::result_match!($lhs, $rhs), "{}", std::format_args!($($arg)+))
    }
}

/// `result_ne` asserts that two given result are equal
#[macro_export]
macro_rules! result_ne {
    ($lhs: expr, $rhs: expr) => {
        std::assert!(!$crate::result_match!($lhs, $rhs))
    };
    ($lhs: expr, $rhs: expr, $($arg:tt)+) => {
        std::assert!($crate::result_match!($lhs, $rhs), "{}", std::format_args!($($arg)+))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error_generator(err: Option<RaftError>) -> Result<usize, RaftError> {
        if let Some(e) = err {
            Err(e)
        } else {
            Ok(42)
        }
    }

    #[test]
    fn test_raft_error_match() {
        result_eq!(
            error_generator(Some(RaftError::InvalidConfig("hello".to_owned()))),
            error_generator(Some(RaftError::InvalidConfig("hello".to_owned())))
        );

        result_ne!(
            error_generator(Some(RaftError::InvalidConfig("hello".to_owned()))),
            error_generator(Some(RaftError::InvalidConfig("world".to_owned())))
        );

        result_eq!(
            error_generator(Some(RaftError::Store(StorageError::Unavailable(21, 42)))),
            error_generator(Some(RaftError::Store(StorageError::Unavailable(21, 42))))
        );

        result_ne!(
            error_generator(Some(RaftError::Store(StorageError::Unavailable(21, 42)))),
            error_generator(Some(RaftError::Store(StorageError::Unavailable(20, 42))))
        );

        result_eq!(
            error_generator(Some(RaftError::Store(StorageError::InvalidIndex(42)))),
            error_generator(Some(RaftError::Store(StorageError::InvalidIndex(42))))
        );

        result_ne!(
            error_generator(Some(RaftError::Store(StorageError::InvalidIndex(42)))),
            error_generator(Some(RaftError::Store(StorageError::InvalidIndex(21))))
        );

        result_eq!(
            error_generator(Some(RaftError::Store(StorageError::EmptyEntries()))),
            error_generator(Some(RaftError::Store(StorageError::EmptyEntries())))
        );

        result_eq!(
            error_generator(Some(RaftError::Log(LogError::InvalidIndex(42)))),
            error_generator(Some(RaftError::Log(LogError::InvalidIndex(42))))
        );

        result_ne!(
            error_generator(Some(RaftError::Log(LogError::InvalidIndex(42)))),
            error_generator(Some(RaftError::Log(LogError::InvalidIndex(21))))
        );

        result_eq!(
            error_generator(Some(RaftError::Log(LogError::Unavailable(21, 42)))),
            error_generator(Some(RaftError::Log(LogError::Unavailable(21, 42))))
        );

        result_ne!(
            error_generator(Some(RaftError::Log(LogError::Unavailable(21, 42)))),
            error_generator(Some(RaftError::Log(LogError::Unavailable(11, 21))))
        );

        result_eq!(
            error_generator(Some(RaftError::Log(LogError::TruncatedStableLog(21, 42)))),
            error_generator(Some(RaftError::Log(LogError::TruncatedStableLog(21, 42))))
        );

        result_ne!(
            error_generator(Some(RaftError::Log(LogError::TruncatedStableLog(21, 42)))),
            error_generator(Some(RaftError::Log(LogError::TruncatedStableLog(11, 21))))
        );

        result_eq!(
            error_generator(Some(RaftError::Log(LogError::Unconsistent(1, 2, 3, 4)))),
            error_generator(Some(RaftError::Log(LogError::Unconsistent(1, 2, 3, 4))))
        );

        result_ne!(
            error_generator(Some(RaftError::Log(LogError::Unconsistent(1, 2, 3, 4)))),
            error_generator(Some(RaftError::Log(LogError::Unconsistent(5, 2, 3, 4))))
        );

        result_ne!(
            error_generator(Some(RaftError::Log(LogError::InvalidIndex(42)))),
            error_generator(Some(RaftError::Store(StorageError::InvalidIndex(21))))
        );

        result_ne!(
            error_generator(Some(RaftError::Log(LogError::EmptyUnstableLog()))),
            error_generator(Some(RaftError::Store(StorageError::EmptyEntries())))
        );
    }
}
