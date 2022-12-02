use crate::{Entry, LogError, RaftError, Storage};
use getset::{Getters, MutGetters};
use std::fmt::Display;

/// Raft log implementation
#[derive(Debug, Getters, MutGetters)]
#[non_exhaustive]
pub struct RaftLog<T: Storage> {
    /// Contains all stable entries since the last snapshot.
    #[getset(get, get_mut)]
    pub store: T,

    /// Contains all unstable entries that will be stored into storage
    #[getset(get)]
    pub unstable_logs: Vec<Entry>,

    /// The highest log position that is known to be in stable storage
    /// on a quorum of nodes.
    ///
    /// Invariant: applied <= committed
    pub committed: u64,

    /// The highest log position that is known to be persisted in stable
    /// storage. It's used for limiting the upper bound of committed and
    /// persisted entries.
    ///
    /// Invariant: persisted < unstable.offset && applied <= persisted
    pub persisted: u64,

    /// The highest log position that the application has been instructed
    /// to apply to its state machine.
    ///
    /// Invariant: applied <= min(committed, persisted)
    #[getset(get)]
    pub applied: u64,
}

impl<T> Display for RaftLog<T>
where
    T: Storage,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "committed = {}, persisted = {}, applied = {}, unstable_logs.len={}",
            self.committed,
            self.persisted,
            self.applied,
            self.unstable_logs.len()
        )
    }
}

impl<T: Storage> RaftLog<T> {
    /// Creates a new raft log with a given storage and tag
    ///
    /// note: `first_index` is always not less than 1, so there is no need to worry about overflow
    #[allow(clippy::integer_arithmetic)]
    #[inline]
    pub fn new(store: T) -> Self {
        let first_index = store.first_index();
        let last_index = store.last_index();
        Self {
            store,
            committed: first_index - 1,
            persisted: last_index,
            applied: first_index - 1,
            unstable_logs: Vec::new(),
        }
    }

    /// Grap the term from the last entry
    ///
    /// # Errors
    ///
    /// if `self.unstable_logs` is empty then return `EmptyUnstableLog`
    #[inline]
    pub fn last_term(&self) -> Result<u64, RaftError> {
        self.term(self.unstable_last_index())
            .map_err(|_e| RaftError::Log(LogError::EmptyUnstableLog()))
    }

    /// Find the term associated with the given index
    ///
    /// # Errors
    ///
    ///
    #[allow(clippy::indexing_slicing, clippy::integer_arithmetic)]
    #[inline]
    pub fn term(&self, idx: u64) -> Result<u64, RaftError> {
        let unstable_first_index = self.unstable_first_index();
        let unstable_last_index = self.unstable_last_index();
        if idx >= self.store.first_index() && idx <= self.store.last_index() {
            self.store.term(idx)
        } else if idx >= unstable_first_index && idx <= unstable_last_index {
            // there is no need to worry about overflow in that offset is belong to [0, self.unstable_logs.len)
            // and it's OK to use offset to index the self.unstable_logs
            let offset: usize = (idx - unstable_first_index)
                .try_into()
                .map_err(|e| RaftError::Others(Box::new(e)))?;
            Ok(self.unstable_logs[offset].term)
        } else {
            Err(RaftError::Log(LogError::IndexOutOfBounds(
                idx,
                unstable_last_index,
            )))
        }
    }

    /// Returns the first index in the unstable log
    #[inline]
    #[allow(clippy::integer_arithmetic)]
    pub fn unstable_first_index(&self) -> u64 {
        match self.unstable_logs.first() {
            Some(e) => e.index,
            // index is a 64-bit unsigned integer number, so the probability of index+1 overflow is very low.
            None => self.store.last_index() + 1,
        }
    }

    /// Returns the last index in the unstable log.
    #[inline]
    pub fn unstable_last_index(&self) -> u64 {
        match self.unstable_logs.last() {
            Some(e) => e.index,
            None => self.store.last_index(),
        }
    }

    /// Appends a set of entries to the unstable list
    ///
    /// # Errors
    ///
    /// if the first index of `ents` is less than `self.committed` then return `TruncatedStableLog`
    /// if the first index of `ents` is greater than `self.unstable_last_index`() + 1 then return `IndexOutOfBounds`
    #[allow(clippy::integer_arithmetic)]
    #[inline]
    pub fn append(&mut self, ents: &[Entry]) -> Result<u64, RaftError> {
        let unstable_last_index = self.unstable_last_index();
        if let Some(ent) = ents.first() {
            let after = ent.index;
            if after <= self.committed {
                return Err(RaftError::Log(LogError::TruncatedStableLog(
                    after,
                    self.committed,
                )));
            }
            if after > unstable_last_index + 1 {
                return Err(RaftError::Log(LogError::IndexOutOfBounds(
                    after,
                    unstable_last_index,
                )));
            }
            let len: usize = (after - self.unstable_first_index() + 1)
                .try_into()
                .map_err(|e| RaftError::Others(Box::new(e)))?;
            self.unstable_logs.truncate(len);
            self.unstable_logs.extend_from_slice(ents);
            Ok(self.unstable_last_index())
        } else {
            Ok(unstable_last_index)
        }
    }

    /// Sets the last committed value to the passed in value.
    ///
    /// # Errors
    ///
    /// return `IndexOutOfBounds` if `to_commit` is larger than the last index of the unstable log
    #[inline]
    pub fn commit_to(&mut self, to_commit: u64) -> Result<(), RaftError> {
        let last_index = self.unstable_last_index();
        if to_commit > last_index {
            return Err(RaftError::Log(LogError::IndexOutOfBounds(
                to_commit, last_index,
            )));
        }
        if to_commit > self.committed {
            self.committed = to_commit;
        }
        Ok(())
    }

    /// Returns the committed index and its term.
    #[inline]
    pub fn commit_info(&self) -> (u64, Option<u64>) {
        match self.term(self.committed) {
            Ok(t) => (self.committed, Some(t)),
            Err(_e) => (self.committed, None),
        }
    }

    /// check the index is match to the term or not
    #[inline]
    pub fn match_term(&self, idx: u64, term: u64) -> bool {
        self.term(idx).map(|t| t == term).unwrap_or(false)
    }

    /// Finds the index of the conflict.
    ///
    /// It returns the first index of conflicting entries between the existing
    /// entries and the given entries, if there are any.
    ///
    /// If there are no conflicting entries, and the existing entries contain
    /// all the given entries, zero will be returned.
    ///
    /// If there are no conflicting entries, but the given entries contains new
    /// entries, the index of the first new entry will be returned.
    ///
    /// An entry is considered to be conflicting if it has the same index but
    /// a different term.
    ///
    /// The first entry MUST have an index equal to the argument 'from'.
    /// The index of the given entries MUST be continuously increasing.
    #[inline]
    pub fn find_conflict(&self, ents: &[Entry]) -> Option<u64> {
        for ent in ents {
            if !self.match_term(ent.index, ent.term) {
                return Some(ent.index);
            }
        }
        None
    }

    /// Clears the unstable entries and moves the stable offset up to the last index, if there is any
    ///
    /// # Errors
    ///
    ///
    #[allow(clippy::integer_arithmetic)]
    #[inline]
    pub fn stable_entries(&mut self, idx: u64, term: u64) -> Result<(), RaftError> {
        let unstable_first_index = self.unstable_first_index();
        let unstable_last_index = self.unstable_last_index();
        if idx < unstable_first_index {
            return Err(RaftError::Log(LogError::IndexOutOfBounds(
                idx,
                unstable_first_index,
            )));
        }
        if idx > unstable_last_index {
            return Err(RaftError::Log(LogError::IndexOutOfBounds(
                unstable_last_index,
                idx,
            )));
        }
        let idx_term = self.term(idx)?;
        if idx_term != term {
            return Err(RaftError::Log(LogError::Unconsistent(
                idx, term, idx, idx_term,
            )));
        }
        // unstable_first_index <= idx <= unstable_last_index, so there's no need to worry about overflow
        let offset: usize = (idx - unstable_first_index)
            .try_into()
            .map_err(|e| RaftError::Others(Box::new(e)))?;

        let drain_res = self.unstable_logs.drain(..=offset);
        self.store.append(drain_res.as_slice())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{storage::MemStorage, Entry};
    fn new_entry(index: u64, term: u64) -> Entry {
        Entry {
            term,
            index,
            ..Default::default()
        }
    }

    #[test]
    fn test_new_raft_log() {
        let storage = MemStorage::new();
        let _res = storage
            .wl()
            .append(&[new_entry(1, 1), new_entry(2, 2)])
            .unwrap();

        let raft_log = RaftLog::new(storage);
        assert_eq!(raft_log.committed, 0);
        assert_eq!(raft_log.persisted, 2);
        assert_eq!(raft_log.applied, 0);
    }

    #[test]
    fn test_raft_log_append() {
        let ents = vec![
            new_entry(1, 1),
            new_entry(2, 2),
            new_entry(3, 3),
            new_entry(4, 4),
        ];
        let tests = vec![
            (
                vec![new_entry(2, 3), new_entry(3, 4)],
                Err(RaftError::Log(LogError::TruncatedStableLog(2, 2))),
            ),
            (vec![new_entry(3, 4)], Ok(3)),
            (
                vec![
                    new_entry(5, 5),
                    new_entry(6, 6),
                    new_entry(7, 7),
                    new_entry(8, 8),
                    new_entry(9, 9),
                ],
                Ok(9),
            ),
            (
                vec![new_entry(7, 7), new_entry(8, 8), new_entry(9, 9)],
                Err(RaftError::Log(LogError::IndexOutOfBounds(7, 4))),
            ),
        ];

        for (entries, wres) in tests {
            let store = MemStorage::new();
            let mut raft_log = RaftLog::new(store);
            raft_log.unstable_logs = ents.clone();
            raft_log.commit_to(2).unwrap();

            let res = raft_log.append(&entries);
            assert_eq!(res, wres);
        }
    }

    #[test]
    fn test_term() {
        let store = MemStorage::new();
        let mut raft_log = RaftLog::new(store);
        let res = raft_log.last_term();
        assert_eq!(res, Err(RaftError::Log(LogError::EmptyUnstableLog())));
        raft_log.unstable_logs = vec![new_entry(1, 2), new_entry(2, 3), new_entry(3, 4)];
        assert_eq!(raft_log.term(2), Ok(3));
        assert_eq!(
            raft_log.term(100),
            Err(RaftError::Log(LogError::IndexOutOfBounds(100, 3)))
        )
    }

    #[test]
    fn test_log_match() {
        let storage = MemStorage::new();
        let mut raft_log = RaftLog::new(storage);
        raft_log.unstable_logs = vec![new_entry(1, 2), new_entry(2, 3), new_entry(3, 4)];
        assert!(raft_log.match_term(1, 2));
        assert!(!raft_log.match_term(2, 2));
        assert!(!raft_log.match_term(21, 21));
    }

    #[test]
    fn test_find_conflict() {
        let ents = vec![new_entry(1, 1), new_entry(2, 2), new_entry(3, 3)];
        let storage = MemStorage::new();
        let mut raft_log = RaftLog::new(storage);
        let _res = raft_log.append(&ents).unwrap();
        let tests = vec![
            (vec![], None),
            (vec![new_entry(1, 1), new_entry(2, 2)], None),
            (
                vec![
                    new_entry(1, 1),
                    new_entry(2, 2),
                    new_entry(3, 3),
                    new_entry(4, 4),
                ],
                Some(4),
            ),
            (vec![new_entry(3, 3), new_entry(4, 4)], Some(4)),
            (vec![new_entry(7, 7), new_entry(8, 8)], Some(7)),
        ];
        for (entires, wres) in tests {
            let res = raft_log.find_conflict(&entires);
            assert_eq!(res, wres);
        }
    }

    #[test]
    fn test_commit_to() {
        let storage = MemStorage::new();
        let mut raft_log = RaftLog::new(storage);
        assert_eq!((0, None), raft_log.commit_info());
        let _res = raft_log.append(&[
            new_entry(1, 1),
            new_entry(2, 2),
            new_entry(3, 3),
            new_entry(4, 4),
        ]);
        raft_log.commit_to(2).unwrap();
        let (idx, term) = raft_log.commit_info();
        assert_eq!((idx, term), (2, Some(2)));
    }

    #[test]
    fn test_stable_entries() {
        let tests = vec![
            (
                vec![new_entry(1, 1), new_entry(2, 2)],
                1,
                1,
                Ok(()),
                vec![new_entry(2, 2)],
            ),
            (vec![new_entry(1, 1), new_entry(2, 2)], 2, 2, Ok(()), vec![]),
            (
                vec![],
                1,
                1,
                Err(RaftError::Log(LogError::IndexOutOfBounds(0, 1))),
                vec![],
            ),
            (
                vec![new_entry(1, 1), new_entry(2, 2)],
                1,
                2,
                Err(RaftError::Log(LogError::Unconsistent(1, 2, 1, 1))),
                vec![new_entry(1, 1), new_entry(2, 2)],
            ),
        ];

        for (ents, idx, term, wres, unstable_logs) in tests {
            let storage = MemStorage::new();
            let mut raft_log = RaftLog::new(storage);
            let _res = raft_log.append(&ents).unwrap();
            assert_eq!(raft_log.unstable_logs, ents);
            let res = raft_log.stable_entries(idx, term);
            assert_eq!(res, wres);
            assert_eq!(raft_log.unstable_logs, unstable_logs);
        }
    }
}
