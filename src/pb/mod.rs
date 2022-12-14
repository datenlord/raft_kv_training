use crate::{ConfState, Entry, Message};
use eraftpb::{
    message::MsgData, MsgAppend, MsgAppendResponse, MsgBeat, MsgHeartbeat, MsgHeartbeatResponse,
    MsgHup, MsgPropose, MsgRequestVote, MsgRequestVoteResponse,
};
use prost::bytes::Bytes;

/// raft message and relevant data structure definition
pub(crate) mod eraftpb;

impl<Iter> From<Iter> for ConfState
where
    Iter: IntoIterator<Item = u64>,
{
    #[must_use]
    #[inline]
    fn from(nodes: Iter) -> Self {
        let mut conf_state = ConfState::default();
        conf_state.peers.extend(nodes.into_iter());
        conf_state
    }
}

impl Entry {
    /// Generate a new `Entry`
    #[must_use]
    #[inline]
    pub fn new(index: u64, term: u64, data: Bytes) -> Self {
        Self { index, term, data }
    }
}

impl Message {
    /// Generate a new hup message
    #[must_use]
    #[inline]
    pub fn new_hup_msg(from: u64, to: u64) -> Self {
        Self {
            msg_data: Some(MsgData::Hup(MsgHup { from, to })),
        }
    }

    /// Generate a new local beat message
    #[must_use]
    #[inline]
    pub fn new_beat_msg(from: u64, to: u64) -> Self {
        Self {
            msg_data: Some(MsgData::Beat(MsgBeat { from, to })),
        }
    }

    /// Generate a new propose message
    #[must_use]
    #[inline]
    pub fn new_propose_msg(from: u64, to: u64, entries: Vec<Entry>) -> Self {
        Self {
            msg_data: Some(MsgData::Propose(MsgPropose { from, to, entries })),
        }
    }

    /// Generate a new propose message
    #[must_use]
    #[inline]
    pub fn new_append_msg(
        from: u64,
        to: u64,
        term: u64,
        leader_commit: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
    ) -> Self {
        Self {
            msg_data: Some(MsgData::Append(MsgAppend {
                from,
                to,
                term,
                leader_commit,
                prev_log_index,
                prev_log_term,
                entries,
            })),
        }
    }

    /// Generate a new append response message
    #[must_use]
    #[inline]
    pub fn new_append_resp_msg(
        from: u64,
        to: u64,
        term: u64,
        reject: bool,
        last_log_index: u64,
        last_log_term: u64,
    ) -> Self {
        Self {
            msg_data: Some(MsgData::AppendResponse(MsgAppendResponse {
                from,
                to,
                term,
                reject,
                last_log_index,
                last_log_term,
            })),
        }
    }

    /// Generate a new heartbeat message
    #[must_use]
    #[inline]
    pub fn new_heartbeat_msg(from: u64, to: u64, term: u64) -> Self {
        Self {
            msg_data: Some(MsgData::Heartbeat(MsgHeartbeat { from, to, term })),
        }
    }

    /// Generate a new heartbeat response message
    #[must_use]
    #[inline]
    pub fn new_heartbeat_resp_msg(from: u64, to: u64, term: u64, reject: bool) -> Self {
        Self {
            msg_data: Some(MsgData::HeartbeatResponse(MsgHeartbeatResponse {
                from,
                to,
                term,
                reject,
            })),
        }
    }

    /// Generate a new request vote message
    #[must_use]
    #[inline]
    pub fn new_request_vote_msg(
        from: u64,
        to: u64,
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
    ) -> Self {
        Self {
            msg_data: Some(MsgData::RequestVote(MsgRequestVote {
                from,
                to,
                term,
                last_log_index,
                last_log_term,
            })),
        }
    }

    /// Generate a new request vote response message
    #[must_use]
    #[inline]
    pub fn new_request_vote_resp_msg(from: u64, to: u64, term: u64, reject: bool) -> Self {
        Self {
            msg_data: Some(MsgData::RequestVoteResponse(MsgRequestVoteResponse {
                from,
                to,
                term,
                reject,
            })),
        }
    }
}
