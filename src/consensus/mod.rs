/// config moduel
pub mod config;
/// raft error module
pub mod errors;
/// raft module
pub mod raft;
/// storage module
pub mod storage;

pub use errors::*;

/// A constant represents invalid id of raft.
pub const INVALID_ID: u64 = 0;

/// A constant represents invalid index of raft log.
pub const INVALID_INDEX: u64 = 0;
