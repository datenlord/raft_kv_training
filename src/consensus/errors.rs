use thiserror::Error;

/// Error Definition in raft
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum RaftError {
    /// Invalid Config
    #[error("{0}")]
    InvalidConfig(String),
    /// RaftLog relevant errors
    #[error("{0}")]
    Log(#[from] LogError),
    /// Some other errors occurred, like storage backend error or type conversion error etc.
    #[error("unknown error {0}")]
    Others(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// An error with the `RaftLog`
#[non_exhaustive]
#[derive(Debug, Error, Clone, Copy)]
pub enum LogError {
    /// Log Unavailable
    #[error("logs from {0} to {1} are unavailable")]
    Unavailable(u64, u64),
    /// Truncated committed log entriees is not allowed
    #[error("Cannot truncate committed log entries [{0}, {1})")]
    TruncateCommittedLog(u64, u64),
    /// Log entry at the give index position is unavailable
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
                ($crate::errors::RaftError::Log(l), $crate::errors::RaftError::Log(r)) => {
                    match (l, r) {
                        (
                            $crate::errors::LogError::LogEntryUnavailable(l),
                            $crate::errors::LogError::LogEntryUnavailable(r),
                        ) => l == r,
                        (
                            $crate::errors::LogError::Unavailable(l1, l2),
                            $crate::errors::LogError::Unavailable(r1, r2),
                        ) => l1 == r1 && l2 == r2,
                        (
                            $crate::errors::LogError::TruncateCommittedLog(l1, l2),
                            $crate::errors::LogError::TruncateCommittedLog(r1, r2),
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
            error_generator(Some(RaftError::Log(LogError::Unavailable(21, 42)))),
            error_generator(Some(RaftError::Log(LogError::Unavailable(21, 42))))
        );

        result_ne!(
            error_generator(Some(RaftError::Log(LogError::Unavailable(21, 42)))),
            error_generator(Some(RaftError::Log(LogError::Unavailable(11, 21))))
        );

        result_eq!(
            error_generator(Some(RaftError::Log(LogError::TruncateCommittedLog(21, 42)))),
            error_generator(Some(RaftError::Log(LogError::TruncateCommittedLog(21, 42))))
        );

        result_ne!(
            error_generator(Some(RaftError::Log(LogError::TruncateCommittedLog(21, 42)))),
            error_generator(Some(RaftError::Log(LogError::TruncateCommittedLog(11, 21))))
        );

        result_ne!(
            error_generator(Some(RaftError::Log(LogError::LogEntryUnavailable(42)))),
            error_generator(Some(RaftError::InvalidConfig("hello world".to_owned())))
        );
    }
}
