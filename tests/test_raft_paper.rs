use prost::bytes::Bytes;
use raft_kv::consensus::raft::*;
use raft_kv::{map, Entry, Message};
use raft_kv::{Config, HardState, MemStorage, Raft, RaftError, Storage, INVALID_ID};
use std::collections::HashMap;
use std::fmt::Debug;

fn new_test_raft(
    id: u64,
    peers: Vec<u64>,
    election: usize,
    heartbeat: usize,
    storage: MemStorage,
) -> Result<Raft<MemStorage>, RaftError> {
    let config = Config::new(id, election, heartbeat);
    if storage.initial_state().initialized() && peers.is_empty() {
        panic!("new_test_raft with empty peers on initialized store");
    }
    if !peers.is_empty() && !storage.initial_state().initialized() {
        storage.initialize_with_conf_state(peers);
    }
    Raft::new(&config, storage)
}

fn empty_entry(term: u64, index: u64) -> Entry {
    Entry::new(index, term, Bytes::new())
}

#[test]
fn start_as_follower_2aa() {
    let config = Config::new(1, 10, 1);
    let store = MemStorage::new();
    let r = Raft::new(&config, store).unwrap();
    assert_eq!(r.role, State::Follower);
}

#[test]
fn test_follower_start_election() {
    test_nonleader_start_election(State::Follower);
}

#[test]
fn test_candidate_start_new_election() {
    test_nonleader_start_election(State::Candidate);
}

// test_nonleader_start_election tests that if a follower receives no communication
// over election timeout, it begins an election to choose a new leader. It
// increments its current term and transitions to candidate state. It then
// votes for itself and issues RequestVote RPCs in parallel to each of the
// other servers in the cluster.
// Reference: section 5.2
// Also if a candidate fails to obtain a majority, it will time out and
// start a new election by incrementing its term and initiating another
// round of RequestVote RPCs.
// Reference: section 5.2
fn test_nonleader_start_election(role: State) {
    // election timeout
    let et = 10;
    let store = MemStorage::new();
    let mut r = new_test_raft(1, vec![1, 2, 3], et, 1, store).unwrap();
    match role {
        State::Follower => r.become_follower(1, 2),
        State::Candidate => r.become_candidate(),
        _ => panic!("Only non-leader role is accepted."),
    }

    for _ in 1..2 * et {
        r.tick();
    }

    assert_eq!(r.term, 2);
    assert_eq!(r.role, State::Candidate);
    assert!(r.votes()[&r.id()]);
    let mut msgs: Vec<Message> = r.msgs.drain(..).collect();
    let last_log_index = r.raft_log.buffer_last_index();
    let last_log_term = r.raft_log.last_term();
    msgs.sort_by_key(|m| format!("{:?}", m));

    let new_message_ext =
        |f, to| Message::new_request_vote_msg(f, to, 2, last_log_term, last_log_index);

    let expect_msgs = vec![new_message_ext(1, 2), new_message_ext(1, 3)];
    assert_eq!(msgs, expect_msgs);
}

#[test]
fn test_follower_election_timeout_randomized() {
    test_non_leader_election_timeout_randomized(State::Follower);
}

#[test]
fn test_candidate_election_timeout_randomized() {
    test_non_leader_election_timeout_randomized(State::Candidate);
}

// test_non_leader_election_timeout_randomized tests that election timeout for
// follower or candidate is randomized.
// Reference: section 5.2
fn test_non_leader_election_timeout_randomized(state: State) {
    let et = 10;
    let store = MemStorage::new();
    let mut r = new_test_raft(1, vec![1, 2, 3], et, 1, store).unwrap();
    let mut timeouts = HashMap::new();
    for _ in 0..1000 * et {
        let term = r.term;
        match state {
            State::Follower => r.become_follower(term + 1, 2),
            State::Candidate => r.become_candidate(),
            _ => panic!("only non leader state is accepted!"),
        }

        let mut time = 0;
        while r.read_messages().is_empty() {
            r.tick();
            time += 1;
        }
        timeouts.insert(time, true);
    }

    assert!(timeouts.len() <= et && timeouts.len() >= et - 1);
    for d in et + 1..2 * et {
        assert!(timeouts[&d]);
    }
}

#[test]
fn test_follower_election_timeout_nonconflict() {
    test_nonleaders_election_timeout_nonconfict(State::Follower);
}

#[test]
fn test_candidates_election_timeout_nonconflict() {
    test_nonleaders_election_timeout_nonconfict(State::Candidate);
}

// test_nonleaders_election_timeout_nonconfict tests that in most cases only a
// single server(follower or candidate) will time out, which reduces the
// likelihood of split vote in the new election.
// Reference: section 5.2
fn test_nonleaders_election_timeout_nonconfict(state: State) {
    let et = 10;
    let size = 5;
    let mut rs = Vec::with_capacity(size);
    let ids: Vec<u64> = (1..=size as u64).collect();
    for id in ids.iter().take(size) {
        rs.push(new_test_raft(*id, ids.clone(), et, 1, MemStorage::new()).unwrap());
    }
    let mut conflicts = 0;
    for _ in 0..1000 {
        for r in &mut rs {
            let term = r.term;
            match state {
                State::Follower => r.become_follower(term + 1, INVALID_ID),
                State::Candidate => r.become_candidate(),
                _ => panic!("non leader state is expect!"),
            }
        }

        let mut timeout_num = 0;
        while timeout_num == 0 {
            for r in &mut rs {
                r.tick();
                if !r.read_messages().is_empty() {
                    timeout_num += 1;
                }
            }
        }
        // several rafts time out at the same tick
        if timeout_num > 1 {
            conflicts += 1;
        }
    }
    assert!(f64::from(conflicts) / 1000.0 <= 0.3);
}

// test_leader_bcast_beat tests that if the leader receives a heartbeat tick,
// it will send a msgApp with m.Index = 0, m.LogTerm=0 and empty entries as
// heartbeat to all followers.
// Reference: section 5.2
#[test]
fn test_leader_bcast_beat() {
    // heartbeat interval
    let hi = 1;
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, hi, MemStorage::new()).unwrap();
    r.become_candidate();
    r.become_leader();
    for i in 0..10u64 {
        let _ = r.append_entry(&mut [empty_entry(0, i + 1)]);
    }

    for _ in 0..hi {
        r.tick();
    }

    let mut msgs = r.read_messages();
    msgs.sort_by_key(|m| format!("{:?}", m));

    let expect_msgs = vec![
        Message::new_heartbeat_msg(1, 2, 1),
        Message::new_heartbeat_msg(1, 3, 1),
    ];
    assert_eq!(msgs, expect_msgs);
}

#[test]
fn test_handle_heartbeat() {
    let successes = vec![
        (
            Message::new_heartbeat_msg(2, 1, 2),
            Message::new_heartbeat_resp_msg(1, 2, 2, false),
            2,
        ),
        (
            Message::new_heartbeat_msg(2, 1, 3),
            Message::new_heartbeat_resp_msg(1, 2, 3, false),
            3,
        ),
    ];
    for (msg, w_reply, w_term) in successes {
        for state in [State::Follower, State::Candidate, State::Leader] {
            let mut r = new_test_raft(1, vec![1, 2], 5, 1, MemStorage::new()).unwrap();
            match state {
                State::Follower => r.become_follower(2, 2),
                State::Candidate => {
                    r.become_follower(1, 2);
                    r.become_candidate();
                }
                State::Leader => {
                    r.become_follower(1, 2);
                    r.become_candidate();
                    r.become_leader();
                }
                _ => unreachable!(),
            }
            r.step(&msg);
            let msgs = r.read_messages();
            assert_eq!(msgs.len(), 1);
            assert_eq!(msgs[0], w_reply);
            assert_eq!(r.term, w_term);
            assert_eq!(r.role, State::Follower);
        }
    }

    for state in [State::Follower, State::Candidate, State::Leader] {
        let mut r = new_test_raft(1, vec![1, 2], 5, 1, MemStorage::new()).unwrap();
        match state {
            State::Follower => r.become_follower(2, 2),
            State::Candidate => {
                r.become_follower(1, 2);
                r.become_candidate();
            }
            State::Leader => {
                r.become_follower(1, 2);
                r.become_candidate();
                r.become_leader();
            }
            _ => unreachable!(),
        }
        r.step(&Message::new_heartbeat_msg(2, 1, 1));
        let msgs = r.read_messages();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], Message::new_heartbeat_resp_msg(1, 2, 2, true));
        assert_eq!(r.term, 2);
        assert_eq!(r.role, state);
    }
}

// test_handle_heartbeat_resp ensures that we re-send log entries when we get a heartbeat response.
#[test]
fn test_handle_heartbeat_resp() {
    let mut r = new_test_raft(1, vec![1, 2], 5, 1, MemStorage::new()).unwrap();
    r.become_candidate();
    r.become_leader();
    r.step(&Message::new_heartbeat_resp_msg(2, 1, 1, false));
    assert_eq!(r.role, State::Leader);
    r.step(&Message::new_heartbeat_resp_msg(2, 1, 2, true));
    assert_eq!(r.role, State::Follower);
}

// test_follower_vote tests that each follower will vote for at most one
// candidate in a given term, on a first-come-first-served basis.
// Reference: section 5.2
#[test]
fn test_follower_vote() {
    let tests = vec![
        (INVALID_ID, 1, false),
        (INVALID_ID, 2, false),
        (1, 1, false),
        (2, 2, false),
        (1, 2, true),
        (2, 1, true),
    ];
    for (vote, nvote, wreject) in tests {
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new()).unwrap();
        r.load_state(&HardState::new(1, vote, 0, 0)).unwrap();
        let msg = Message::new_request_vote_msg(
            nvote,
            1,
            1,
            r.raft_log.buffer_last_index(),
            r.raft_log.last_term(),
        );
        r.step(&msg);
        let msgs = r.read_messages();
        let reply = Message::new_request_vote_resp_msg(1, nvote, 1, wreject);
        let expect_msgs = vec![reply];
        assert_eq!(msgs, expect_msgs);
    }
}

// test_voter tests the voter denies its vote if its own log is more up-to-date
// than that of the candidate.
// Reference: section 5.4.1
#[test]
fn test_voter() {
    let tests = vec![
        // same logterm
        (vec![empty_entry(1, 1)], 1, 1, false),
        (vec![empty_entry(1, 1)], 1, 2, false),
        (vec![empty_entry(1, 1), empty_entry(1, 2)], 1, 1, true),
        // candidate higher logterm
        (vec![empty_entry(1, 1)], 2, 1, false),
        (vec![empty_entry(1, 1)], 2, 2, false),
        (vec![empty_entry(1, 1), empty_entry(1, 2)], 2, 1, false),
        // voter higher logterm
        (vec![empty_entry(2, 1)], 1, 1, true),
        (vec![empty_entry(2, 1)], 1, 2, true),
        (vec![empty_entry(2, 1), empty_entry(2, 2)], 1, 1, true),
    ];

    for (ents, log_term, index, wreject) in tests {
        let s = MemStorage::new();
        s.wl().append(&ents).unwrap();
        let mut r = new_test_raft(1, vec![1, 2], 10, 1, s).unwrap();
        let m = Message::new_request_vote_msg(2, 1, 3, index, log_term);
        r.step(&m);

        let msgs = r.read_messages();
        assert!(msgs.len() == 1);
        assert_eq!(
            msgs[0],
            Message::new_request_vote_resp_msg(1, 2, 3, wreject)
        );
    }
}

#[test]
fn test_vote_from_any_state() {
    for state in [State::Follower, State::Candidate, State::Leader] {
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new()).unwrap();
        r.term = 1;
        match state {
            State::Follower => {
                let term = r.term;
                r.become_follower(term, 3);
            }
            State::Candidate => r.become_candidate(),
            State::Leader => {
                r.become_candidate();
                r.become_leader();
            }
            _ => unreachable!(),
        }
        // Note that setting our state above may have advanced r.term
        // past its initial value.
        let new_term = r.term + 1;

        let msg = Message::new_request_vote_msg(2, 1, new_term, 42, new_term);
        r.step(&msg);
        let msgs = r.read_messages();

        assert_eq!(msgs.len(), 1);
        assert_eq!(
            msgs[0],
            Message::new_request_vote_resp_msg(1, 2, new_term, false)
        );

        assert_eq!(r.role, State::Follower);
        assert_eq!(r.vote, 2);
    }
}

// test_leader_election_in_one_round_rpc tests all cases that may happen in
// leader election during one round of RequestVote RPC:
// a) it wins the election
// b) it loses the election
// c) it is unclear about the result
// Reference: section 5.2
#[test]
fn test_leader_election_in_one_round_rpc() {
    let tests = vec![
        // win the election when receiving votes from a majority of the servers
        (1, map!(), State::Leader),
        (3, map!(2 => true, 3 => true), State::Leader),
        (3, map!(2 => true), State::Leader),
        (
            5,
            map!(2 => true, 3 => true, 4 => true, 5 => true),
            State::Leader,
        ),
        (5, map!(2 => true, 3 => true, 4 => true), State::Leader),
        (5, map!(2 => true, 3 => true), State::Leader),
        // return to follower state if it receives vote denial from a majority
        (3, map!(2 => false, 3 => false), State::Follower),
        (
            5,
            map!(2 => false, 3 => false, 4 => false, 5 => false),
            State::Follower,
        ),
        (
            5,
            map!(2 => true, 3 => false, 4 => false, 5 => false),
            State::Follower,
        ),
        // stay in candidate if it does not obtain the majority
        (3, map!(), State::Candidate),
        (5, map!(2 => true), State::Candidate),
        (5, map!(2 => false, 3 => false), State::Candidate),
        (5, map!(), State::Candidate),
    ];

    for (size, votes, state) in tests {
        let mut r =
            new_test_raft(1, (1..=size as u64).collect(), 10, 1, MemStorage::new()).unwrap();

        r.step(&Message::new_hup_msg(1, 1));
        for (id, vote) in votes {
            let m = Message::new_request_vote_resp_msg(id, 1, r.term, !vote);
            r.step(&m);
        }
        assert_eq!(r.role, state);
        assert_eq!(r.term, 1);
    }
}

// test_candidate_reset_term tests when a candidate receives a
// MsgHeartbeat from leader, "step" resets the term
// with leader's and reverts back to follower.
#[test]
fn test_candidate_reset_term_msg_heartbeat() {
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new()).unwrap();
    r.become_candidate();
    r.step(&Message::new_heartbeat_msg(2, 1, 3));
    assert_eq!(r.role, State::Follower);
}

#[test]
fn test_campaign_while_leader() {
    let mut r = new_test_raft(1, vec![1], 5, 1, MemStorage::new()).unwrap();
    assert_eq!(r.role, State::Follower);
    // We don't call campaign() directly because it comes after the check
    // for our current state.
    r.step(&Message::new_hup_msg(1, 1));
    assert_eq!(r.role, State::Leader);
    let term = r.term;
    r.step(&Message::new_hup_msg(1, 1));
    assert_eq!(r.role, State::Leader);
    assert_eq!(r.term, term);
}
