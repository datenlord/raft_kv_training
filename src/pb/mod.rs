use crate::ConfState;
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
        conf_state.nodes.extend(nodes.into_iter());
        conf_state
    }
}
