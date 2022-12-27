use crate::errors::RaftError;

/// type conversion helper
///
/// # Errors
///
/// return `RaftError::Others` if the conversion failed.
#[inline]
pub fn down_cast<U, T: TryFrom<U>>(oprand: U) -> Result<T, RaftError>
where
    <T as TryFrom<U>>::Error: std::error::Error + Send + Sync + 'static,
{
    oprand
        .try_into()
        .map_err(|e| RaftError::Others(Box::new(e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_down_cast() {
        let op_1 = 10u64;
        let res: usize = down_cast(op_1).unwrap();
        assert_eq!(res, 10);
    }
}

/// Get the count of macro's arguments.
///
/// # Examples
///
/// ```
/// # use raft_kv::count_args;
/// # fn main() {
/// assert_eq!(count_args!(), 0);
/// assert_eq!(count_args!(1), 1);
/// assert_eq!(count_args!(1, 2), 2);
/// assert_eq!(count_args!(1, 2, 3), 3);
/// # }
/// ```
#[macro_export]
macro_rules! count_args {
    () => { 0 };
    ($head:expr $(, $tail:expr)*) => { 1 + raft_kv::count_args!($($tail),*) };
}

/// Initial a `HashMap` with specify key-value pairs.
///
/// # Examples
///
/// ```
/// # use raft_kv::map;
/// # fn main() {
/// // empty map
/// let m: std::collections::HashMap<u8, u8> = map!();
/// assert!(m.is_empty());
///
/// // one initial kv pairs.
/// let m = map!("key" => "value");
/// assert_eq!(m.len(), 1);
/// assert_eq!(m["key"], "value");
///
/// // initialize with multiple kv pairs.
/// let m = map!("key1" => "value1", "key2" => "value2");
/// assert_eq!(m.len(), 2);
/// assert_eq!(m["key1"], "value1");
/// assert_eq!(m["key2"], "value2");
/// # }
/// ```
#[macro_export]
macro_rules! map {
    () => {
        {
            use std::collections::HashMap;
            HashMap::new()
        }
    };
    ( $( $k:expr => $v:expr ),+ ) => {
        {
            use std::collections::HashMap;
            let mut temp_map = HashMap::with_capacity(raft_kv::count_args!($(($k, $v)),+));
            $(
                temp_map.insert($k, $v);
            )+
            temp_map
        }
    };
}
