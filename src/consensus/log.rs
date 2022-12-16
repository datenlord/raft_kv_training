use crate::{down_cast, Entry, LogError, RaftError, Storage, INVALID_INDEX, INVALID_TERM};
use getset::{Getters, MutGetters};
use std::fmt::Display;

/// Raft log implementation
#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Getters, MutGetters)]
#[non_exhaustive]
pub struct RaftLog<T: Storage> {
    /// Contains all persisted entries
    #[getset(get, get_mut)]
    pub store: T,

    /// Contains all unstable entries that will be stored into storage
    #[getset(get)]
    pub log_buffer: Vec<Entry>,

    /// The highest log position that is known to be in stable storage
    /// on a quorum of nodes.
    ///
    /// Invariant: applied <= committed
    pub committed: u64,

    /// The highest log position that is known to be persisted in stable
    /// storage. It's used for limiting the upper bound of committed and
    /// persisted entries.
    ///
    /// Invariant: committed <= persisted
    pub persisted: u64,

    /// The highest log position that the application has been instructed
    /// to apply to its state machine.
    ///
    /// Invariant: applied <= committed <= persisted
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
            "committed = {}, persisted = {}, applied = {}, log_buffer.len={}",
            self.committed,
            self.persisted,
            self.applied,
            self.log_buffer.len()
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
        let last_index = store.last_index();
        let raft_state = store.initial_state();
        Self {
            store,
            committed: raft_state.hard_state().committed,
            persisted: last_index,
            applied: raft_state.hard_state().applied,
            log_buffer: Vec::new(),
        }
    }

    /// Grap the term from the last entry
    #[inline]
    pub fn last_term(&self) -> u64 {
        match self.log_buffer.last() {
            Some(ent) => ent.term,
            None => {
                let last_index = self.store.last_index();

                match self.store.term(last_index) {
                    Ok(v) => v,
                    Err(_e) => unreachable!(),
                }
            }
        }
    }

    /// Find the term associated with the given index
    ///
    /// # Errors
    ///
    /// return `IndexOutOfBounds` if the idx is out of the range
    /// [`buffer_first_index`, `buffer_last_index`] or [`stable_first_index`, `stable_last_index`]
    #[allow(clippy::indexing_slicing, clippy::integer_arithmetic)]
    #[inline]
    pub fn term(&self, idx: u64) -> Result<u64, RaftError> {
        if idx == INVALID_INDEX {
            return Ok(INVALID_TERM);
        }
        let buffer_first_index = self.buffer_first_index();
        let buffer_last_index = self.buffer_last_index();
        let storage_first_idx = self.store.first_index();
        let storage_last_idx = self.store.last_index();
        if idx >= storage_first_idx && idx <= storage_last_idx {
            self.store.term(idx)
        } else if idx >= buffer_first_index && idx <= buffer_last_index {
            // there is no need to worry about overflow in that offset is belong to [0, self.log_buffer.len)
            // and it's OK to use offset to index the self.log_buffer
            let offset: usize = down_cast(idx - buffer_first_index)?;
            Ok(self.log_buffer[offset].term)
        } else {
            Err(RaftError::Log(LogError::LogEntryUnavailable(idx)))
        }
    }

    /// Returns the first index in the unstable log
    #[inline]
    #[allow(clippy::integer_arithmetic)]
    pub fn buffer_first_index(&self) -> u64 {
        match self.log_buffer.first() {
            Some(e) => e.index,
            // index is a 64-bit unsigned integer number, so the probability of index+1 overflow is very low.
            None => self.store.last_index() + 1,
        }
    }

    /// Returns the last index in the unstable log.
    #[inline]
    pub fn buffer_last_index(&self) -> u64 {
        match self.log_buffer.last() {
            Some(e) => e.index,
            None => self.store.last_index(),
        }
    }

    /// Appends a set of entries to the unstable list
    ///
    /// Note: `buffer_first` = persisted + 1
    ///   first     committed   persisted `buffer_first`    `buffer_last`
    ///    |____________|____________|________|__________________|
    ///    |<---------storage------->|        |<--`log_buffer`-->|
    /// 1      |_ents_|
    /// 2          |_ents_|
    /// 3                  |_ents_|   
    /// 4                         |_____ents_____|   
    /// 5                                          |_ents_|     
    /// 6                                               |____ents_____|    
    /// 7                                                              |__ents__|
    ///
    /// # Errors
    ///
    /// if the first index of `ents` is less than `self.committed` then return `TruncateCommittedLog`
    /// if the first index of `ents` is greater than `self.buffer_last_index`() + 1 then return `Unavailable`
    #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)]
    #[inline]
    pub fn append(&mut self, ents: &[Entry]) -> Result<(), RaftError> {
        if ents.is_empty() {
            return Ok(());
        }

        let buffer_last_idx = self.buffer_last_index();
        let ents_first_idx = ents[0].index;
        let ents_last_idx = ents[ents.len() - 1].index;

        if ents_first_idx <= self.committed {
            // case 1 and case 2
            return Err(RaftError::Log(LogError::TruncateCommittedLog(
                ents_first_idx,
                self.committed,
            )));
        }

        if ents_first_idx > buffer_last_idx + 1 {
            // case 7
            return Err(RaftError::Log(LogError::Unavailable(
                buffer_last_idx + 1,
                ents_first_idx,
            )));
        }

        if ents_first_idx <= self.persisted {
            if ents_last_idx <= self.persisted {
                // case 3
                self.store.append(ents)?;
            } else {
                // case 4, we need to split ents into two tow parts.
                let offset: usize = down_cast(self.persisted - ents_first_idx)?;
                self.store.append(&ents[0..=offset])?;
                self.log_buffer.clear();
                self.log_buffer.extend_from_slice(&ents[offset + 1..]);
            }
        } else {
            // case 5 and case 6
            let length: usize = down_cast(ents_first_idx - self.persisted)?;
            self.log_buffer.truncate(length);
            self.log_buffer.extend_from_slice(ents);
        }

        Ok(())
    }

    /// Sets the last committed value to the passed in value.
    ///
    /// # Errors
    ///
    /// return `IndexOutOfBounds` if `to_commit` is larger than the last index of the unstable log
    #[allow(clippy::integer_arithmetic)]
    #[inline]
    pub fn commit_to(&mut self, to_commit: u64) -> Result<(), RaftError> {
        let last_index = self.buffer_last_index();
        if to_commit > last_index {
            return Err(RaftError::Log(LogError::Unavailable(
                last_index + 1,
                to_commit,
            )));
        }
        if to_commit > self.committed {
            self.committed = to_commit;
        }
        Ok(())
    }

    /// Returns the committed index and its term.
    #[inline]
    pub fn commit_info(&self) -> (u64, u64) {
        match self.term(self.committed) {
            Ok(t) => (self.committed, t),
            Err(_e) => {
                unreachable!()
            }
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

    /// Store the log buffer to the storage backend, if there is any
    #[allow(clippy::integer_arithmetic)]
    #[inline]
    pub fn persist_log_buffer(&mut self) {
        let log_buffer_temp = self.log_buffer.drain(..);
        match self.store.append(log_buffer_temp.as_slice()) {
            Ok(()) => self.persisted = self.store.last_index(),
            Err(_) => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{result_eq, storage::MemStorage, Entry, INVALID_TERM};
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
                Err(RaftError::Log(LogError::TruncateCommittedLog(2, 2))),
            ),
            (vec![new_entry(3, 4)], Ok(())),
            (
                vec![
                    new_entry(5, 5),
                    new_entry(6, 6),
                    new_entry(7, 7),
                    new_entry(8, 8),
                    new_entry(9, 9),
                ],
                Ok(()),
            ),
            (
                vec![new_entry(7, 7), new_entry(8, 8), new_entry(9, 9)],
                Err(RaftError::Log(LogError::Unavailable(5, 7))),
            ),
        ];

        for (entries, wres) in tests {
            let store = MemStorage::new();
            let mut raft_log = RaftLog::new(store);
            raft_log.log_buffer = ents.clone();
            raft_log.commit_to(2).unwrap();

            let res = raft_log.append(&entries);
            result_eq!(res, wres);
        }
    }

    #[test]
    fn test_term() {
        let store = MemStorage::new();
        let mut raft_log = RaftLog::new(store);
        let res = raft_log.last_term();
        assert_eq!(res, INVALID_TERM);
        raft_log.log_buffer = vec![new_entry(1, 2), new_entry(2, 3), new_entry(3, 4)];
        result_eq!(raft_log.term(2), Ok(3));
        result_eq!(
            raft_log.term(100),
            Err(RaftError::Log(LogError::LogEntryUnavailable(100)))
        )
    }

    #[test]
    fn test_log_match() {
        let storage = MemStorage::new();
        let mut raft_log = RaftLog::new(storage);
        raft_log.log_buffer = vec![new_entry(1, 2), new_entry(2, 3), new_entry(3, 4)];
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
        assert_eq!((0, 0), raft_log.commit_info());
        let _res = raft_log.append(&[
            new_entry(1, 1),
            new_entry(2, 2),
            new_entry(3, 3),
            new_entry(4, 4),
        ]);
        raft_log.commit_to(2).unwrap();
        let (idx, term) = raft_log.commit_info();
        assert_eq!((idx, term), (2, 2));
    }

    #[test]
    fn test_persist_logentries() {
        let tests = vec![
            (
                vec![new_entry(1, 1), new_entry(2, 2)],
                vec![new_entry(3, 3), new_entry(4, 4)],
                4,
            ),
            (vec![], vec![new_entry(1, 1), new_entry(2, 2)], 2),
            (vec![new_entry(1, 1), new_entry(2, 2)], vec![], 2),
            (vec![], vec![], 0),
        ];

        for (stables, log_buffer, persisted) in tests {
            let storage = MemStorage::new();
            storage.wl().append(&stables[..]).unwrap();
            let mut raft_log = RaftLog::new(storage);
            raft_log.append(&log_buffer).unwrap();
            raft_log.persist_log_buffer();
            assert!(raft_log.log_buffer.is_empty());
            assert_eq!(raft_log.persisted, persisted);
        }
    }
}
