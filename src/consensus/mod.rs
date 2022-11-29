/// config moduel
pub mod config;
/// raft error module
pub mod errors;
/// raft module
pub mod raft;

pub use errors::*;

/// A constant represents invalid id of raft.
pub const INVALID_ID: u64 = 0;
