use crate::sql::SqlResult;
use std::ops::{Bound, RangeBounds};

/// Store is unaware of the version of the key, this logic must be decided inside the
/// implementation of MVCC
pub trait Store {
    /// Deletes a key, or does nothing if it does not exist.
    fn delete(&mut self, key: &[u8]) -> SqlResult<()>;

    /// Flushes any buffered data to the underlying storage medium.
    fn flush(&mut self) -> SqlResult<()>;

    /// Gets a value for a key, if it exists.
    fn get(&self, key: &[u8]) -> SqlResult<Option<Vec<u8>>>;

    /// Iterates over an ordered range of key/value pairs.
    fn scan(&self, range: Range) -> Scan;

    /// Sets a value for a key, replacing the existing value if any.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> SqlResult<()>;
}

pub struct Range {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
}
impl Range {
    /// Creates a new range from the given Rust range. We can't use the RangeBounds directly in
    /// scan() since that prevents us from using Store as a trait object. Also, we can't take
    /// AsRef<[u8]> or other convenient types, since that won't work with e.g. .. ranges.
    pub fn from<R: RangeBounds<Vec<u8>>>(range: R) -> Self {
        Self {
            start: match range.start_bound() {
                Bound::Included(v) => Bound::Included(v.to_vec()),
                Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
            end: match range.end_bound() {
                Bound::Included(v) => Bound::Included(v.to_vec()),
                Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
        }
    }

    /// Checks if the given value is contained in the range.
    fn contains(&self, v: &[u8]) -> bool {
        (match &self.start {
            Bound::Included(start) => &**start <= v,
            Bound::Excluded(start) => &**start < v,
            Bound::Unbounded => true,
        }) && (match &self.end {
            Bound::Included(end) => v <= &**end,
            Bound::Excluded(end) => v < &**end,
            Bound::Unbounded => true,
        })
    }
}
pub type Scan = Box<dyn DoubleEndedIterator<Item = SqlResult<(Vec<u8>, Vec<u8>)>>>;
