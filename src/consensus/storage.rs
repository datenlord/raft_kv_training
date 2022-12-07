use crate::{ConfState, Entry, HardState, RaftError, StorageError, INVALID_INDEX};
use getset::{Getters, MutGetters, Setters};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Holds both the hard state {term, commit index, vote leader} and the configuration state
#[derive(Debug, Clone, Default, Getters, Setters, MutGetters)]
pub struct RaftState {
    /// Contains the last meta information including commit index, the vote leader, and the vote term.
    #[getset(get, set, get_mut)]
    hard_state: HardState,
    /// Records the current node IDs like `[1, 2, 3]` in the cluster.
    #[getset(set)]
    conf_state: ConfState,
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

    /// Returns a slice of log entries in the range `[low, high)`.
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
    /// check whether there is an entry at the given index of `entries`
    #[inline]
    fn has_entry_at(&self, index: u64) -> Result<(), RaftError> {
        let last_idx = self.last_index();
        if index > last_idx {
            return Err(RaftError::Store(StorageError::Unavailable(last_idx, index)));
        }
        Ok(())
    }

    /// get the index of the first entry in the `entries`
    fn first_index(&self) -> u64 {
        /// When `self.entries` is empty, `FIRST_INDEX` represents the first valid entry index
        /// `FIRST_INDEX` = `INVALID_INDEX` + 1
        const FIRST_INDEX: u64 = 1;
        match self.entries.first() {
            Some(e) => e.index,
            None => FIRST_INDEX,
        }
    }

    /// get the index of the last entry in the `entries`
    fn last_index(&self) -> u64 {
        match self.entries.last() {
            Some(e) => e.index,
            None => INVALID_INDEX,
        }
    }

    /// Append the new entries to storage.
    ///
    /// # Errors
    ///
    /// return `RaftError` if the index of the first entry in the `entries`is larger than the `last_index`()
    #[inline]
    #[allow(clippy::integer_arithmetic)]
    pub fn append(&mut self, ents: &[Entry]) -> Result<(), RaftError> {
        if let Some(first_entry) = ents.first() {
            let first_index = self.first_index();
            // TODO: consider raft log compacted

            let last_index = self.last_index();
            if last_index + 1 < first_entry.index {
                return Err(RaftError::Store(StorageError::Unavailable(
                    last_index,
                    first_entry.index,
                )));
            }

            // Remove all entries overwritten by `ents`.
            let diff = first_entry.index - first_index;
            let diff: usize = diff
                .try_into()
                .map_err(|e| RaftError::Others(Box::new(e)))?;
            {
                let _drain_res = self.entries.drain(diff..);
            }
            self.entries.extend_from_slice(ents);
        }

        Ok(())
    }

    /// Commit to an index.
    ///
    /// # Errors
    ///
    /// return `RaftError` if there is no such entry in raft logs.
    #[inline]
    #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)]
    pub fn commit_to(&mut self, index: u64) -> Result<(), RaftError> {
        self.has_entry_at(index)?;
        // It's OK to allow `indexing_slicing` in that the previous `has_entry_at` has provided
        // that the zero and `offset` is safe to be used as a index of `self.entries`.
        let offset: usize = (index - self.first_index())
            .try_into()
            .map_err(|e| RaftError::Others(Box::new(e)))?;

        let term = self.entries[offset].term;
        let hard_state = self.raft_state_mut().hard_state_mut();
        hard_state.commit = index;
        hard_state.term = term;
        Ok(())
    }

    /// Commit to `idx` and set configuration to the given states. Only used for tests.
    ///
    /// # Errors
    ///
    /// return `RaftError` if there is no such entry in raft logs.
    #[inline]
    pub fn commit_to_and_set_conf_states(
        &mut self,
        idx: u64,
        cs: Option<ConfState>,
    ) -> Result<(), RaftError> {
        self.commit_to(idx)?;
        if let Some(cs) = cs {
            let _res = self.raft_state.set_conf_state(cs);
        }
        Ok(())
    }
}

/// `MemStorage` is a thread-safe but incomplete implementation of `Storage`, mainly for tests.
///
/// A real `Storage` should save both raft logs and applied data. However `MemStorage` only
/// contains raft logs. So you can call `MemStorage::append` to persist new received unstable raft
/// logs and then access them with `Storage` APIs.
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
        let core = self.wl();
        let last_idx = core.last_index();
        if high > last_idx + 1 {
            return Err(RaftError::Store(StorageError::Unavailable(
                last_idx + 1,
                high,
            )));
        }

        let offset = core.entries[0].index;
        let lo = (low - offset)
            .try_into()
            .map_err(|e| RaftError::Others(Box::new(e)))?;
        let hi = (high - offset)
            .try_into()
            .map_err(|e| RaftError::Others(Box::new(e)))?;
        Ok(core.entries[lo..hi].to_vec())
    }

    #[inline]
    fn first_index(&self) -> u64 {
        self.rl().first_index()
    }

    #[inline]
    fn last_index(&self) -> u64 {
        self.rl().last_index()
    }

    #[allow(clippy::indexing_slicing, clippy::integer_arithmetic)]
    #[inline]
    fn term(&self, index: u64) -> Result<u64, RaftError> {
        let core = self.rl();
        core.has_entry_at(index)?;
        let offset: usize = (index - core.first_index())
            .try_into()
            .map_err(|e| RaftError::Others(Box::new(e)))?;
        Ok(core.entries[offset].term)
    }

    #[inline]
    fn append(&mut self, ents: &[Entry]) -> Result<(), RaftError> {
        self.wl().append(ents)
    }
}

#[cfg(test)]
mod test {
    use crate::{Entry, HardState, RaftError, StorageError};

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
            (
                4,
                Err(RaftError::Store(crate::StorageError::Unavailable(3, 4))),
            ),
        ];

        let storage = MemStorage::new();
        storage.wl().entries = ents;

        for (idx, wterm) in tests {
            let t = storage.term(idx);
            assert_eq!(t, wterm);
        }
    }

    #[test]
    fn test_storage_entries() {
        let ents = vec![
            new_entry(1, 1),
            new_entry(2, 2),
            new_entry(3, 3),
            new_entry(4, 4),
        ];
        let tests = vec![
            (2, 6, Err(RaftError::Store(StorageError::Unavailable(5, 6)))),
            (2, 3, Ok(vec![new_entry(2, 2)])),
            (3, 4, Ok(vec![new_entry(3, 3)])),
            (
                2,
                5,
                Ok(vec![new_entry(2, 2), new_entry(3, 3), new_entry(4, 4)]),
            ),
        ];
        let storage = MemStorage::new();
        storage.wl().entries = ents;
        for (low, high, wentries) in tests {
            let e = storage.entries(low, high);
            assert_eq!(e, wentries);
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
                None,
            ),
            // truncate the existing entries and append
            (
                vec![new_entry(2, 3)],
                Some(vec![new_entry(1, 2), new_entry(2, 3)]),
            ),
        ];

        for (entries, wentries) in tests {
            let storage = MemStorage::new();
            storage.wl().entries = ents.clone();
            let res = storage.wl().append(&entries);
            match res {
                Ok(_) => {
                    if let Some(wentries) = wentries {
                        assert_eq!(storage.wl().entries, wentries);
                    }
                }
                Err(e) => {
                    assert_eq!(e, RaftError::Store(crate::StorageError::Unavailable(3, 7)));
                }
            }
        }
    }

    #[test]
    fn test_storage_commit_to_and_set_conf_state() {
        let ents = vec![new_entry(1, 1), new_entry(2, 2), new_entry(3, 3)];
        let tests: Vec<(u64, Option<HardState>)> = vec![
            (
                2,
                Some(HardState {
                    term: 2,
                    commit: 2,
                    vote: 0,
                }),
            ),
            (5, None),
        ];

        for (idx, hs) in tests {
            let storage = MemStorage::new();
            storage.wl().entries = ents.clone();
            let res = storage.wl().commit_to(idx);
            match res {
                Ok(_) => {
                    if let Some(hs) = hs {
                        assert_eq!(storage.rl().raft_state.hard_state, hs);
                    }
                }
                Err(e) => assert_eq!(e, RaftError::Store(StorageError::Unavailable(3, idx))),
            }
        }
    }
}
