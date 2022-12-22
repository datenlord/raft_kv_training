use crate::{
    down_cast, ConfState, Entry, HardState, LogError, RaftError, INVALID_INDEX, INVALID_TERM,
};
use getset::{Getters, MutGetters, Setters};
use std::cmp::{max, min};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Holds both the hard state {term, commit index, vote leader} and the configuration state
#[non_exhaustive]
#[derive(Debug, Clone, Default, Getters, Setters, MutGetters)]
pub struct RaftState {
    /// Contains the last meta information including commit index, the vote leader, and the vote term.
    #[getset(get = "pub", set, get_mut)]
    pub hard_state: HardState,
    /// Records the current node IDs like `[1, 2, 3]` in the cluster.
    #[getset(set)]
    pub conf_state: ConfState,
}

impl RaftState {
    /// Creates a new `RaftState`
    #[inline]
    #[must_use]
    pub fn new(hard_state: HardState, conf_state: ConfState) -> Self {
        Self {
            hard_state,
            conf_state,
        }
    }

    /// Indicates the `RaftState` is initialized or not.
    #[inline]
    #[must_use]
    pub fn initialized(&self) -> bool {
        self.conf_state != ConfState::default()
    }
}

/// Storage saves all the information about the current Raft implementation, including Raft Log,
/// commit index, the leader to vote for, etc.
///
/// If any Storage method returns an error, the raft instance will
/// become inoperable and refuse to participate in elections; the
/// application is responsible for cleanup and recovery in this case.
pub trait Storage {
    /// `initial_state` is called when Raft is initialized. This interface will return a `RaftState`
    /// which contains `HardState` and `ConfState`.
    ///
    /// `RaftState` could be initialized or not. If it's initialized it means the `Storage` is
    /// created with a configuration, and its last index and term should be greater than 0.
    fn initial_state(&self) -> RaftState;

    /// Returns a slice of log entries in the range `[low, high]`.
    /// The slice of entries returned will always have length at least 1 if entries are
    /// found in the range.
    ///
    /// # Errors
    ///
    /// Return `Unavailable` if `high` is higher than `Storage::last_index(&self) + 1`.
    /// Return `Others` if type conversion is failed
    fn entries(&self, low: u64, high: u64) -> Result<Vec<Entry>, RaftError>;

    /// Returns the term of entry idx, which must be in the range
    /// [`first_index`()-1, `last_index`()]. The term of the entry before
    /// `first_index` is retained for matching purpose even though the
    /// rest of that entry may not be available.
    ///
    /// # Errors
    ///
    /// return `Unavailable` if index is larger than `self.last_index`()
    fn term(&self, index: u64) -> Result<u64, RaftError>;

    /// Returns the index of the first log entry that is possible available via entries, which will
    /// always equal to `truncated index` plus 1.
    ///
    /// New created (but not initialized) `Storage` can be considered as truncated at 0 so that 1
    /// will be returned in this case.
    fn first_index(&self) -> u64;

    /// The index of the last entry replicated in the `Storage`.
    fn last_index(&self) -> u64;

    /// Store a entry slice into the `Storage`
    /// # Errors
    ///
    /// return `RaftError` if the index of the first entry in the `entries`is larger than the `last_index`()
    fn append(&mut self, ents: &[Entry]) -> Result<(), RaftError>;
}

/// The Memory Storage Core instance holds the actual state of the storage struct. To access this
/// value, use the `rl` and `wl` functions on the main `MemStorage` implementation.
#[derive(Debug, Default, Getters, Setters, MutGetters)]
pub struct MemStorageCore {
    /// The raft state
    #[getset(get, set, get_mut)]
    raft_state: RaftState,
    /// entries[i] has raft log position i+snapshot.get_metadata().index
    entries: Vec<Entry>,
}

impl MemStorageCore {
    /// get the entry from the given index
    ///
    /// # Errors
    ///
    /// return `LogEntryUnavailable` if `entries`
    #[allow(clippy::indexing_slicing, clippy::integer_arithmetic)]
    #[inline]
    fn get_entry(&self, idx: u64) -> Result<&Entry, RaftError> {
        let first_idx = self.first_index();
        let last_idx = self.last_index();
        match (first_idx, last_idx) {
            (Some(first), Some(last)) => {
                if idx <= last {
                    let offset: usize = down_cast(idx - first)?;
                    Ok(&self.entries[offset])
                } else {
                    Err(RaftError::Log(LogError::LogEntryUnavailable(idx)))
                }
            }
            (None, Some(_) | None) | (Some(_), None) => {
                Err(RaftError::Log(LogError::LogEntryUnavailable(idx)))
            }
        }
    }

    /// get the index of the first entry in the `entries`
    fn first_index(&self) -> Option<u64> {
        self.entries.first().map(|e| e.index)
    }

    /// get the index of the last entry in the `entries`
    fn last_index(&self) -> Option<u64> {
        self.entries.last().map(|e| e.index)
    }

    /// Append the new entries to storage.
    ///
    /// # Errors
    ///
    /// return `RaftError` if the index of the first entry in the `entries`is larger than the `last_index`()
    #[inline]
    #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)]
    pub fn append(&mut self, ents: &[Entry]) -> Result<(), RaftError> {
        if ents.is_empty() {
            return Ok(());
        }
        let ents_first_idx = ents[0].index;
        let first_idx = self
            .first_index()
            .map_or_else(|| INVALID_INDEX + 1, |first| first);

        let last_idx = self.last_index().map_or_else(|| INVALID_INDEX, |last| last);

        if last_idx + 1 < ents_first_idx {
            Err(RaftError::Log(LogError::Unavailable(
                last_idx,
                ents_first_idx,
            )))
        } else {
            let offset: usize = down_cast(ents_first_idx - first_idx)?;
            drop(self.entries.drain(offset..));
            self.entries.extend_from_slice(ents);
            Ok(())
        }
    }

    /// Commit to an index.
    ///
    /// # Errors
    ///
    /// return `RaftError` if there is no such entry in raft logs.
    #[inline]
    #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)]
    pub fn commit_to(&mut self, idx: u64) -> Result<(), RaftError> {
        if idx == INVALID_INDEX {
            return Ok(());
        }
        let entry = self.get_entry(idx)?;

        let term = entry.term;
        let hard_state = self.raft_state_mut().hard_state_mut();
        hard_state.committed = idx;
        hard_state.term = term;
        Ok(())
    }
}

/// `MemStorage` is a thread-safe but incomplete implementation of `Storage`, mainly for tests.
///
/// A real `Storage` should save both raft logs and applied data. However `MemStorage` only
/// contains raft logs. So you can call `MemStorage::append` to persist new received unstable raft
/// logs and then access them with `Storage` APIs.
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Default, Debug)]
pub struct MemStorage {
    /// The core of the storage
    core: Arc<RwLock<MemStorageCore>>,
}

impl MemStorage {
    /// Returns a new `MemStorage`
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// Create a new `MemStorage` with a given `Config`. The given `Config` will be used to
    /// initialize the storage.
    ///
    /// You should use the same input to initialize all nodes.
    #[inline]
    pub fn new_with_conf_state<T>(conf_state: T) -> Self
    where
        ConfState: From<T>,
    {
        let store = Self::new();
        store.initialize_with_conf_state(conf_state);
        store
    }

    /// Initialize a `MemStorage` with a given `Config`.
    ///
    /// You should use the same input to initialize all nodes.
    #[inline]
    pub fn initialize_with_conf_state<T>(&self, conf_state: T)
    where
        ConfState: From<T>,
    {
        let mut core = self.wl();
        let _res = core.raft_state.set_conf_state(ConfState::from(conf_state));
    }

    /// Opens up a read lock on the storage and returns a guard handle. Use this
    /// with functions that don't require mutation.
    #[inline]
    pub fn rl(&self) -> RwLockReadGuard<'_, MemStorageCore> {
        #[allow(clippy::expect_used)]
        self.core.read().expect("the RwLock is poisoned")
    }

    /// Opens up a write lock on the storage and returns guard handle. Use this
    /// with functions that take a mutable reference to self.
    #[inline]
    pub fn wl(&self) -> RwLockWriteGuard<'_, MemStorageCore> {
        #[allow(clippy::expect_used)]
        self.core.write().expect("the RwLock is poisoned")
    }
}

impl Storage for MemStorage {
    #[inline]
    fn initial_state(&self) -> RaftState {
        self.rl().raft_state.clone()
    }

    #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)]
    #[inline]
    fn entries(&self, low: u64, high: u64) -> Result<Vec<Entry>, RaftError> {
        if low > high {
            return Ok(vec![]);
        }

        let first_idx = self.first_index();
        let last_idx = self.last_index();
        if first_idx == INVALID_INDEX + 1 && last_idx == INVALID_INDEX {
            Err(RaftError::Log(LogError::Unavailable(low, high)))
        } else {
            let core = self.wl();
            if low < first_idx {
                return Err(RaftError::Log(LogError::Unavailable(
                    low,
                    min(high, first_idx - 1),
                )));
            }

            if high > last_idx {
                return Err(RaftError::Log(LogError::Unavailable(
                    max(low, last_idx + 1),
                    high,
                )));
            }

            let lo: usize = down_cast(low - first_idx)?;
            let hi: usize = down_cast(high - first_idx)?;
            Ok(core.entries[lo..=hi].to_vec())
        }
    }

    #[inline]
    #[allow(clippy::integer_arithmetic)]
    fn first_index(&self) -> u64 {
        self.rl()
            .first_index()
            .map_or_else(|| INVALID_INDEX + 1, |first| first)
    }

    #[inline]
    fn last_index(&self) -> u64 {
        self.rl()
            .last_index()
            .map_or_else(|| INVALID_INDEX, |last| last)
    }

    #[allow(clippy::indexing_slicing, clippy::integer_arithmetic)]
    #[inline]
    fn term(&self, idx: u64) -> Result<u64, RaftError> {
        if idx == INVALID_INDEX {
            return Ok(INVALID_TERM);
        }
        let core = self.rl();
        let entry = core.get_entry(idx)?;
        Ok(entry.term)
    }

    #[inline]
    fn append(&mut self, ents: &[Entry]) -> Result<(), RaftError> {
        self.wl().append(ents)
    }
}

#[cfg(test)]
mod test {
    use std::vec;

    use crate::{result_eq, Entry, HardState, LogError, RaftError};

    use super::{MemStorage, Storage};

    fn new_entry(index: u64, term: u64) -> Entry {
        Entry {
            index,
            term,
            ..Default::default()
        }
    }

    #[test]
    fn test_storage_term() {
        let ents = vec![new_entry(1, 4), new_entry(2, 5), new_entry(3, 6)];
        let tests: Vec<(u64, Result<u64, RaftError>)> = vec![
            (1, Ok(4)),
            (2, Ok(5)),
            (3, Ok(6)),
            (4, Err(RaftError::Log(LogError::LogEntryUnavailable(4)))),
        ];

        let storage = MemStorage::new();
        storage.wl().entries = ents;

        for (idx, wterm) in tests {
            let t = storage.term(idx);
            result_eq!(t, wterm);
        }
    }

    #[test]
    fn test_storage_entries() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let tests_1 = vec![
            (2, 1, Ok(vec![])),
            (1, 2, Err(RaftError::Log(LogError::Unavailable(1, 2)))),
            (2, 4, Err(RaftError::Log(LogError::Unavailable(2, 2)))),
            (2, 3, Err(RaftError::Log(LogError::Unavailable(2, 2)))),
            (4, 4, Ok(vec![new_entry(4, 4)])),
            (3, 4, Ok(vec![new_entry(3, 3), new_entry(4, 4)])),
            (
                3,
                5,
                Ok(vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)]),
            ),
            (4, 6, Err(RaftError::Log(LogError::Unavailable(6, 6)))),
            (6, 7, Err(RaftError::Log(LogError::Unavailable(6, 7)))),
            (1, 7, Err(RaftError::Log(LogError::Unavailable(1, 2)))),
        ];
        let storage = MemStorage::new();
        storage.wl().entries = ents;
        for (low, high, wentries) in tests_1 {
            let e = storage.entries(low, high);
            result_eq!(e, wentries);
        }

        let tests_2 = vec![
            (1, 2, Err(RaftError::Log(LogError::Unavailable(1, 2)))),
            (4, 5, Err(RaftError::Log(LogError::Unavailable(4, 5)))),
            (1, 10, Err(RaftError::Log(LogError::Unavailable(1, 10)))),
            (2, 1, Ok(vec![])),
        ];
        let storage = MemStorage::new();
        for (low, high, wentries) in tests_2 {
            let e = storage.entries(low, high);
            result_eq!(e, wentries);
        }
    }

    #[test]
    fn test_storage_first_index() {
        let ents = vec![new_entry(1, 4), new_entry(2, 5), new_entry(3, 6)];
        let storage = MemStorage::new();
        storage.wl().entries = ents;
        assert_eq!(storage.first_index(), 1);
        // TODO: considering the compact case
    }

    #[test]
    fn test_storage_last_index() {
        let ents = vec![new_entry(1, 4), new_entry(2, 5), new_entry(3, 6)];
        let storage = MemStorage::new();
        storage.wl().entries = ents;
        assert_eq!(storage.last_index(), 3);
        let res = storage.wl().append(&[new_entry(4, 7), new_entry(5, 8)]);
        match res {
            Ok(_) => assert_eq!(storage.last_index(), 5),
            Err(_) => unreachable!(),
        }
    }

    #[test]
    fn test_sotrage_append() {
        let ents = vec![new_entry(1, 2), new_entry(2, 2), new_entry(3, 3)];
        let tests = vec![
            (
                vec![
                    new_entry(3, 5),
                    new_entry(4, 6),
                    new_entry(5, 7),
                    new_entry(6, 8),
                ],
                Ok(()),
                Some(vec![
                    new_entry(1, 2),
                    new_entry(2, 2),
                    new_entry(3, 5),
                    new_entry(4, 6),
                    new_entry(5, 7),
                    new_entry(6, 8),
                ]),
            ),
            // direct append
            (
                vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)],
                Ok(()),
                Some(vec![
                    new_entry(1, 2),
                    new_entry(2, 2),
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ]),
            ),
            // append entries whose index is greater than the last_index is not allowed
            (
                vec![new_entry(7, 3), new_entry(8, 3), new_entry(9, 5)],
                Err(RaftError::Log(LogError::Unavailable(3, 7))),
                None,
            ),
            // truncate the existing entries and append
            (
                vec![new_entry(2, 3)],
                Ok(()),
                Some(vec![new_entry(1, 2), new_entry(2, 3)]),
            ),
        ];

        for (entries, wres, wentries) in tests {
            let storage = MemStorage::new();
            storage.wl().entries = ents.clone();
            let res = storage.wl().append(&entries);
            result_eq!(res, wres);
            if let Some(wentries) = wentries {
                assert_eq!(storage.wl().entries, wentries);
            }
        }
    }

    #[test]
    fn test_storage_commit_to_and_set_conf_state() {
        let ents = vec![new_entry(1, 1), new_entry(2, 2), new_entry(3, 3)];
        let tests: Vec<(u64, Option<HardState>, Result<(), RaftError>)> = vec![
            (0, None, Ok(())),
            (
                2,
                Some(HardState {
                    term: 2,
                    committed: 2,
                    voted_for: 0,
                    applied: 0,
                }),
                Ok(()),
            ),
            (
                5,
                None,
                Err(RaftError::Log(LogError::LogEntryUnavailable(5))),
            ),
        ];

        for (idx, hs, wres) in tests {
            let storage = MemStorage::new();
            storage.wl().entries = ents.clone();
            let res = storage.wl().commit_to(idx);
            result_eq!(res, wres);
            if let Some(hs) = hs {
                assert_eq!(storage.rl().raft_state.hard_state, hs);
            }
        }
    }
}
