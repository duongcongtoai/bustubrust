use crate::sql::DataType;
use std::sync::Arc;

pub type ColumnRef = Arc<dyn Columned>;

/// More reference at: https://docs.rs/arrow/latest/arrow/array/trait.Array.html
/// or at: https://github.com/datafuselabs/databend/blob/cf6da3ead9111b8bfa893cd5c0957cdabf149e2b/common/datavalues/src/columns/column.rs#L33
pub trait Columned: Send + Sync {
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn null_at(&self, _row: usize) -> bool {
        false
    }
    fn memory_size(&self) -> usize;

    fn type_id(&self) -> DataType;

    fn arc(&self) -> ColumnRef;
    // fn as_arrow_array(&self) -> ArrayRef;
    fn slice(&self, offset: usize, length: usize) -> ColumnRef;
    // fn filter(&self, filter: &BooleanColumn) -> ColumnRef;

    /// scatter() partitions the input array into multiple arrays.
    /// indices: a slice whose length is the same as the array.
    /// The element of indices indicates which group the corresponding row
    /// in the input array belongs to.
    /// scattered_size: the number of partitions
    ///
    /// Example: if the input array has four rows [1, 2, 3, 4] and
    /// _indices = [0, 1, 0, 1] and _scatter_size = 2,
    /// then the output would be a vector of two arrays: [1, 3] and [2, 4].
    fn scatter(&self, indices: &[usize], scattered_size: usize) -> Vec<ColumnRef>;
}

/// Borrow from arrow crate
macro_rules! hash_array_primitive {
    ($array_type:ident, $column: ident, $ty: ident, $hashes: ident, $random_state: ident, $multi_col: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        let values = array.values();

        if $multi_col {
            for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                *hash = combine_hashes($ty::get_hash(value, $random_state), *hash);
            }
        } else {
            for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                *hash = $ty::get_hash(value, $random_state)
            }
        }
    };
}
pub(crate) use hash_array_primitive;

// Combines two hashes into one hash
#[inline]
fn combine_hashes(l: u64, r: u64) -> u64 {
    let hash = (17 * 37u64).wrapping_add(l);
    hash.wrapping_mul(37).wrapping_add(r)
}
