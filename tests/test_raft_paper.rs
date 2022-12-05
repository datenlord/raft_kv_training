use raft_kv::config::Config;
use raft_kv::consensus::raft::*;

use std::fmt::Debug;

fn error_handle<T, E>(res: Result<T, E>) -> T
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

#[test]
fn start_as_follower_2aa() {
    let config = Config::new(1, 10, 1);
    let r = error_handle(Raft::new(&config));
    assert_eq!(r.state, State::Follower);
}
