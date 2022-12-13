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
