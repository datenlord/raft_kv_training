/// config moduel
pub mod config;
/// raft error module
pub mod errors;
/// log module
pub mod log;
/// raft module
pub mod raft;
/// storage module
pub mod storage;

pub use config::*;
pub use errors::*;
pub use raft::*;
pub use storage::*;

/// A constant represents invalid id of raft.
pub const INVALID_ID: u64 = 0;

/// A constant represents invalid index of raft log.
pub const INVALID_INDEX: u64 = 0;

/// A constant represents invalid term of raft log.
pub const INVALID_TERM: u64 = 0;
