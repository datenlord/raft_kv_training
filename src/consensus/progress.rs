/// Progress represents a follower’s progress in the view of the leader. Leader maintains
/// progresses of all followers, and sends entries to the follower based on its progress.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Progress {
    /// The last log index
    pub log_index: u64,
    /// The last log term
    pub log_term: u64,
}

impl Progress {
    /// Creates a new progress
    #[must_use]
    #[inline]
    pub fn new(log_index: u64, log_term: u64) -> Self {
        Self {
            log_index,
            log_term,
        }
    }
}
