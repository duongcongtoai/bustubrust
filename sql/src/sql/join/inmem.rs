use crate::sql::{
    exe::{BoxedDataIter, DataIter, ExecutionContext, Operator, PlanType, SchemaDataIter},
    join::{
        grace::PartitionedQueue,
        hash_util::{create_hashes, hash_to_buckets},
    },
    DataBlock, Error as SqlError, SqlResult,
};
use ahash::RandomState;
use datafusion::{
    arrow::{
        array::{
            Array, ArrayData, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array,
            Int32Array, Int64Array, Int8Array, LargeStringArray, PrimitiveArray, StringArray,
            TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
            TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt64BufferBuilder,
            UInt8Array,
        },
        compute::take,
        datatypes::{DataType, SchemaRef, TimeUnit, UInt64Type},
    },
    physical_plan::{
        expressions::Column,
        join_utils::{ColumnIndex, JoinSide},
    },
};
// use futures::{Stream, StreamExt};
use futures_core::Stream;
use futures_util::StreamExt;
use hashbrown::raw::RawTable;
use smallvec::{smallvec, SmallVec};
use std::{
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

type JoinedTable = RawTable<(u64, SmallVec<[u64; 1]>)>;

#[derive(Debug)]
pub struct HashJoinOp {
    on_left: Vec<Column>,
    on_right: Vec<Column>,
    outer_queue: Arc<dyn PartitionedQueue>,
    inner_queue: Arc<dyn PartitionedQueue>,
    p_index: usize,
    batch_size: usize,
    built: bool,
    schema: SchemaRef,
    join_column_indices: Vec<ColumnIndex>,
    random_state: RandomState,
}

impl Operator for HashJoinOp {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn execute_sync(&mut self, _: ExecutionContext) -> SqlResult<BoxedDataIter> {
        let mut inner_table = JoinedTable::new();
        let inner_data = self.inner_queue.dequeue_all(self.p_index)?;

        let mut hash_buffer = Vec::new();
        let offset = 0;
        hash_buffer.clear();
        hash_buffer.resize(inner_data.num_rows(), 0);
        Self::hash_batch_and_store(
            &self.on_right,
            &mut inner_table,
            &inner_data,
            offset,
            &mut hash_buffer,
            &self.random_state,
        )?;
        // Ok((inner_batch, join_table))

        let outer_stream: BoxedDataIter =
            self.outer_queue.dequeue(self.p_index, self.batch_size)?;

        return Ok(Box::new(HashJoiner {
            outer_stream,
            on_left: self.on_left.clone(),
            on_right: self.on_right.clone(),
            inner_data,
            inner_table,
            hash_state: self.random_state.clone(),
            join_column_indices: self.join_column_indices.clone(),
            schema: self.schema.clone(),
        }));
    }
}

pub struct HashJoiner {
    outer_stream: BoxedDataIter,
    on_left: Vec<Column>,
    on_right: Vec<Column>,
    inner_data: DataBlock,
    inner_table: JoinedTable,
    hash_state: RandomState,
    join_column_indices: Vec<ColumnIndex>,
    schema: SchemaRef,
}
unsafe impl Sync for HashJoiner {}
unsafe impl Send for HashJoiner {}

impl DataIter for HashJoiner {
    fn schema(self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Iterator for HashJoiner {
    type Item = SqlResult<DataBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.outer_stream.next() {
            Some(batch_ret) => {
                match batch_ret {
                    Ok(outer_batch) => {
                        let outer_joined_values = self
                            .on_left
                            .iter()
                            .map(|c| outer_batch.column(c.index()).clone())
                            .collect::<Vec<_>>();
                        let inner_join_values = self
                            .on_right
                            .iter()
                            .map(|c| self.inner_data.column(c.index()).clone())
                            .collect::<Vec<_>>();
                        let mut hash_buffer = vec![0; outer_joined_values[0].len()];
                        let outer_hash_values =
                            create_hashes(&outer_joined_values, &self.hash_state, &mut hash_buffer)
                                .unwrap();
                        let mut outer_indices = UInt64BufferBuilder::new(0);
                        let mut inner_indices = UInt64BufferBuilder::new(0);

                        for (outer_row, hash_value) in outer_hash_values.iter().enumerate() {
                            if let Some((_, indices)) = self
                                .inner_table
                                .get(*hash_value, |(hash, _)| *hash_value == *hash)
                            {
                                // equal hash, need to check real value
                                for inner_row in indices {
                                    if equal_rows(
                                        *inner_row as usize,
                                        outer_row,
                                        &inner_join_values,
                                        &outer_joined_values,
                                        false,
                                    )
                                    .unwrap()
                                    {
                                        outer_indices.append(outer_row as u64);
                                        inner_indices.append(*inner_row as u64);
                                    }
                                }
                            }
                        }
                        let inner = ArrayData::builder(DataType::UInt64)
                            .len(inner_indices.len())
                            .add_buffer(inner_indices.finish())
                            .build()
                            .unwrap();
                        let inner_indices = PrimitiveArray::<UInt64Type>::from(inner);

                        let outer = ArrayData::builder(DataType::UInt64)
                            .len(outer_indices.len())
                            .add_buffer(outer_indices.finish())
                            .build()
                            .unwrap();
                        let outer_indices = PrimitiveArray::<UInt64Type>::from(outer);
                        let next_batch = build_batch_from_indices(
                            &self.schema,
                            &outer_batch,
                            &self.inner_data,
                            outer_indices,
                            inner_indices,
                            &self.join_column_indices,
                        )
                        .unwrap();
                        Some(Ok(next_batch))
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            None => None,
        }
    }
}

impl HashJoinOp {
    pub fn new(
        on_left: Vec<Column>,
        on_right: Vec<Column>,
        outer_queue: Arc<dyn PartitionedQueue>,
        inner_queue: Arc<dyn PartitionedQueue>,
        schema: SchemaRef,
        p_index: usize,
        random_state: RandomState,
        batch_size: usize,
        join_column_indices: Vec<ColumnIndex>,
    ) -> Self {
        HashJoinOp {
            on_left,
            on_right,
            outer_queue,
            inner_queue,
            p_index,
            random_state,
            batch_size,
            built: false,
            join_column_indices,
            schema,
        }
    }

    fn make_joined_columes(joined_on: &[Column], data: &DataBlock) -> Vec<ArrayRef> {
        joined_on
            .iter()
            .map(|col_info| data.column(col_info.index()).clone())
            .collect::<Vec<_>>()
    }

    fn hash_batch_and_store(
        joined_on: &[Column],
        htable: &mut JoinedTable,
        batch: &DataBlock,
        offset: usize,
        reused_hash_buffer: &mut Vec<u64>,
        hash_state: &RandomState,
    ) -> SqlResult<()> {
        let joined_columnar = Self::make_joined_columes(joined_on, batch);
        let hash_values = create_hashes(&joined_columnar, hash_state, reused_hash_buffer)?;
        for (row, hash_value) in hash_values.iter().enumerate() {
            let item = htable.get_mut(*hash_value, |(hash, _)| *hash_value == *hash);
            if let Some((_, indices)) = item {
                indices.push((row + offset) as u64);
            } else {
                htable.insert(
                    *hash_value,
                    (*hash_value, smallvec![(row + offset) as u64]),
                    |(hash, _)| *hash,
                );
            }
        }
        Ok(())
    }

    fn _build<'a>(
        p_index: usize,
        inner_queue: Arc<dyn PartitionedQueue>,
        random_state: &'a RandomState,
        on_right: &'a Vec<Column>,
    ) -> SqlResult<(DataBlock, JoinedTable)> {
        let mut join_table = JoinedTable::new();
        let inner_batch = inner_queue.dequeue_all(p_index)?;

        let mut hash_buffer = Vec::new();
        let offset = 0;
        hash_buffer.clear();
        hash_buffer.resize(inner_batch.num_rows(), 0);
        Self::hash_batch_and_store(
            on_right,
            &mut join_table,
            &inner_batch,
            offset,
            &mut hash_buffer,
            random_state,
        )?;
        Ok((inner_batch, join_table))
    }
}

/// For example
/// outer: [1,2,3,4] [...] [...]
/// inner: [2,3,4,5] [...]
/// outer_indices:  [1,2,3] (since 2,3,4 match)
/// inner_indices:  [0,1,2] (since 2,3,4 match)
/// column_indices: [(0,left),(1,left),(2,left),(0,right),(1,right)]:
/// => Result:  
fn build_batch_from_indices(
    schema: &SchemaRef,
    outer: &DataBlock,
    inner: &DataBlock,
    outer_indices: UInt64Array,
    inner_indices: UInt64Array,
    column_indices: &[ColumnIndex],
) -> SqlResult<DataBlock> {
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());
    for col_index in column_indices {
        let col_values = match col_index.side {
            JoinSide::Left => {
                let col_values = outer.column(col_index.index);
                take(col_values.as_ref(), &outer_indices, None)?
            }
            JoinSide::Right => {
                let col_values = inner.column(col_index.index);
                take(col_values.as_ref(), &inner_indices, None)?
            }
        };
        columns.push(col_values);
    }
    Ok(DataBlock::try_new(schema.clone(), columns)?)
}

macro_rules! equal_rows_elem {
    ($array_type:ident, $l: ident, $r: ident, $left: ident, $right: ident, $null_equals_null: ident) => {{
        let left_array = $l.as_any().downcast_ref::<$array_type>().unwrap();
        let right_array = $r.as_any().downcast_ref::<$array_type>().unwrap();

        match (left_array.is_null($left), right_array.is_null($right)) {
            (false, false) => left_array.value($left) == right_array.value($right),
            (true, true) => $null_equals_null,
            _ => false,
        }
    }};
}

fn equal_rows(
    left: usize,
    right: usize,
    left_arrays: &[ArrayRef],
    right_arrays: &[ArrayRef],
    null_equals_null: bool,
) -> SqlResult<bool> {
    let mut err = None;
    let res = left_arrays
        .iter()
        .zip(right_arrays)
        .all(|(l, r)| match l.data_type() {
            DataType::Null => true,
            DataType::Boolean => {
                equal_rows_elem!(BooleanArray, l, r, left, right, null_equals_null)
            }
            DataType::Int8 => {
                equal_rows_elem!(Int8Array, l, r, left, right, null_equals_null)
            }
            DataType::Int16 => {
                equal_rows_elem!(Int16Array, l, r, left, right, null_equals_null)
            }
            DataType::Int32 => {
                equal_rows_elem!(Int32Array, l, r, left, right, null_equals_null)
            }
            DataType::Int64 => {
                equal_rows_elem!(Int64Array, l, r, left, right, null_equals_null)
            }
            DataType::UInt8 => {
                equal_rows_elem!(UInt8Array, l, r, left, right, null_equals_null)
            }
            DataType::UInt16 => {
                equal_rows_elem!(UInt16Array, l, r, left, right, null_equals_null)
            }
            DataType::UInt32 => {
                equal_rows_elem!(UInt32Array, l, r, left, right, null_equals_null)
            }
            DataType::UInt64 => {
                equal_rows_elem!(UInt64Array, l, r, left, right, null_equals_null)
            }
            DataType::Float32 => {
                equal_rows_elem!(Float32Array, l, r, left, right, null_equals_null)
            }
            DataType::Float64 => {
                equal_rows_elem!(Float64Array, l, r, left, right, null_equals_null)
            }
            DataType::Timestamp(time_unit, None) => match time_unit {
                TimeUnit::Second => {
                    equal_rows_elem!(TimestampSecondArray, l, r, left, right, null_equals_null)
                }
                TimeUnit::Millisecond => {
                    equal_rows_elem!(
                        TimestampMillisecondArray,
                        l,
                        r,
                        left,
                        right,
                        null_equals_null
                    )
                }
                TimeUnit::Microsecond => {
                    equal_rows_elem!(
                        TimestampMicrosecondArray,
                        l,
                        r,
                        left,
                        right,
                        null_equals_null
                    )
                }
                TimeUnit::Nanosecond => {
                    equal_rows_elem!(
                        TimestampNanosecondArray,
                        l,
                        r,
                        left,
                        right,
                        null_equals_null
                    )
                }
            },
            DataType::Utf8 => {
                equal_rows_elem!(StringArray, l, r, left, right, null_equals_null)
            }
            DataType::LargeUtf8 => {
                equal_rows_elem!(LargeStringArray, l, r, left, right, null_equals_null)
            }
            _ => {
                // This is internal because we should have caught this before.
                err = Some(Err(SqlError::Value("something wrong".to_string())));
                false
            }
        });

    err.unwrap_or(Ok(res))
}
