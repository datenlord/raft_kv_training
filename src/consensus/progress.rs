/// Progress represents a follower’s progress in the view of the leader. Leader maintains
/// progresses of all followers, and sends entries to the follower based on its progress.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Progress {
    /// How much state is matched
    pub matched: u64,
    /// The next index to apply
    pub next_idx: u64,
    /// Committed index in raft_log
    pub committed_index: u64,
}

impl Progress {
    /// Creates a new progress
    #[must_use]
    #[inline]
    pub fn new(next_idx: u64) -> Self {
        Self {
            next_idx,
            ..Default::default()
        }
    }
}
