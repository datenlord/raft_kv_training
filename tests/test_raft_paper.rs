use raft_kv::consensus::raft::*;
use raft_kv::Message;
use raft_kv::{Config, MemStorage, Raft, RaftError, Storage};
use std::collections::HashMap;
use std::fmt::Debug;

pub fn error_handle<T, E>(res: Result<T, E>) -> T
where
    E: Debug,
{
    match res {
        Ok(t) => t,
        Err(e) => {
            unreachable!("{:?}", e)
        }
    }
}

pub fn new_test_raft(
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

#[test]
fn start_as_follower_2aa() {
    let config = Config::new(1, 10, 1);
    let store = MemStorage::new();
    let r = error_handle(Raft::new(&config, store));
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
    assert!(r.votes()[&r.id]);
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
