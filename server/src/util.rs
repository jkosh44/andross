//! Various utilities.

pub(crate) fn u64_to_usize(n: u64) -> usize {
    n.try_into().expect("64-bit platform")
}

#[cfg(test)]
pub(crate) fn usize_to_u64(n: usize) -> u64 {
    n.try_into().expect("64-bit platform")
}
