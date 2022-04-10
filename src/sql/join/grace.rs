use super::inmem::HashJoiner;
use crate::sql::{
    exe::{Executor, Operator, PlanType, SchemaStream, SendableDataBlockStream},
    join::hash_util::hash_to_buckets,
    ColumnInfo, DataBlock, ExecutionContext, Schema, SqlResult,
};
use ahash::RandomState;
use arrow::{array::Array, datatypes::SchemaRef, error::Result as ArrowResult};
use async_stream::{try_stream, AsyncStream};
use datafusion::physical_plan::{
    expressions::Column,
    join_utils::{ColumnIndex, JoinSide},
};
use futures::{Future, FutureExt, StreamExt};
use itertools::Itertools;
use std::{collections::HashMap, fmt::Debug, sync::Arc};

#[allow(dead_code)]

pub struct GraceHashJoinPlan {
    on_left: Vec<Column>,
    on_right: Vec<Column>,
    left_plan: Box<PlanType>,
    right_plan: Box<PlanType>,
}
// type JoinedTable = RawTable<(u64, SmallVec<[u64; 1]>)>;

// TODO: add fallback to merge join, if partition contain duplicate joined rows count
// that takes more than inmem partition
#[derive(Debug)]
pub struct GraceHashJoiner<'a, F>
where
    F: Fn() -> Arc<dyn PartitionedQueue>,
{
    config: Config<'a>,
    stack: Vec<PartitionLevel>,
    undone_joining_partition: Option<HashJoiner>,
    queue_allocator: F,
    join_column_indices: Vec<ColumnIndex>,
    schema: SchemaRef,
}
unsafe impl<'a, F> Send for GraceHashJoiner<'a, F> where F: Fn() -> Arc<dyn PartitionedQueue> {}
unsafe impl<'a, F> Sync for GraceHashJoiner<'a, F> where F: Fn() -> Arc<dyn PartitionedQueue> {}

#[async_trait::async_trait]
impl<F, 'a> Operator for GraceHashJoiner<F, 'a>
where
    F: Fn() -> Arc<dyn PartitionedQueue> + Send + Sync + Debug,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn execute(&mut self, ctx: ExecutionContext) -> SqlResult<SendableDataBlockStream> {
        let stream = try_stream! {
            'recursiveloop: while self.stack.len() > 0 {
                while let Some((p_index, cur_level, outer_queue, inner_queue)) =
                    self._find_next_inmem_sized_partition(self.config.max_size_per_partition)
                {
                    let mut inmem_joiner = self._new_hash_joiner(
                        p_index,
                        cur_level,
                        self.schema.clone(),
                        outer_queue,
                        inner_queue,
                    );
                    let inmem_stream = inmem_joiner.execute(ctx).await?;
                    while let Some(datablock) = inmem_stream.next().await {
                        yield Ok(datablock?)
                    }
                }
                while let Some((p_index, outer_queue, inner_queue)) =
                    self._find_next_fallback_partition()
                {
                    // p_index is a partition that even if partition one more time, it may not fit in
                    // memory, so we use fallback strategy like merge join instead
                    // make this fallback operation as late as possible
                    panic!("unimplemented")
                }

                let cur_partitions = self.stack.last_mut().unwrap();

                // recursive partition
                if cur_partitions.map.len() > 0 {
                    // let st = cur_partitions.last_mut();
                    if let Some(next_recursive_p) =
                        Self::recursive_partition(&self.queue_allocator, self.config, cur_partitions)
                            .await?
                    {
                        self.stack.push(next_recursive_p);
                        continue 'recursiveloop;
                    }
                }
                self.stack.pop();
            }
        };
        let schema_stream = SchemaStream::new(self.schema.clone(), stream);
        Ok(Box::pin(schema_stream))
    }
}
#[derive(Debug)]
struct PartitionLevel {
    level: usize,
    map: HashMap<usize, PInfo>,
    fallback_partitions: Vec<usize>,
    outer_queue: Arc<dyn PartitionedQueue>,
    inner_queue: Arc<dyn PartitionedQueue>,
}

#[async_trait::async_trait]
pub trait PartitionedQueue: Sync + Send + Debug {
    async fn enqueue(&self, partition_idx: usize, data: DataBlock) -> SqlResult<()>;

    async fn dequeue_all(&self, partition_idx: usize) -> SqlResult<DataBlock>;

    async fn dequeue(
        &self,
        partition_idx: usize,
        size: usize,
    ) -> SqlResult<SendableDataBlockStream>;
    fn id(&self) -> usize;
}

struct PInfo {
    parent_size: usize,
    memsize: usize,
}

#[derive(Copy, Clone)]
struct Config<'a> {
    bucket_size: usize,
    max_size_per_partition: usize,
    batch_size: usize,
    left_key_offset: usize,
    on_left: &'a [Column],
    on_right: &'a [Column],
}
fn build_join_schema(left: &Schema, right: &Schema) -> (Schema, Vec<ColumnIndex>) {
    let left_fields = left
        .fields()
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, field)| {
            (
                field,
                ColumnIndex {
                    index,
                    side: JoinSide::Left,
                },
            )
        });
    let right_fields = right
        .fields()
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, field)| {
            (
                field,
                ColumnIndex {
                    index,
                    side: JoinSide::Right,
                },
            )
        });
    let (combined_fields, joined_column_indices) = left_fields.chain(right_fields).unzip();
    (Schema::new(combined_fields), joined_column_indices)
}

impl<'a, F> GraceHashJoiner<'a, F>
where
    F: Fn() -> Arc<dyn PartitionedQueue>,
{
    /// A big todo here
    pub fn from_plan(plan: GraceHashJoinPlan, f: F, ctx: ExecutionContext) -> Self {
        let left_op = Executor::create_from_subplan_operator(*plan.left_plan, ctx.clone());
        let right_op = Executor::create_from_subplan_operator(*plan.right_plan, ctx.clone());
        let default_config = Config {
            bucket_size: 10,
            max_size_per_partition: 10,
            batch_size: 10,
            left_key_offset: 4,
            on_left: &plan.on_left,
            on_right: &plan.on_right,
        };
        let (left_schema, right_schema) = (left_op.schema(), right_op.schema());

        let (schema, column_indices) = build_join_schema(&left_schema, &right_schema);

        return GraceHashJoiner::new(
            default_config,
            left_op,
            right_op,
            f,
            column_indices,
            Arc::new(schema),
        )
        .expect("failted to create grace hash joiner");
    }

    async fn partition_batch<'b>(
        batch_stream: &'b mut SendableDataBlockStream,
        partition_infos: &mut PartitionLevel,
        c: Config<'b>,
        is_inner: bool,
    ) -> SqlResult<()> {
        let mut on = c.on_left;
        if is_inner {
            on = c.on_right;
        }

        let queuer = match is_inner {
            true => &partition_infos.inner_queue,
            false => &partition_infos.outer_queue,
        };

        let mut reused_buffer = Vec::new();
        while let Some(batch) = batch_stream.next().await {
            let batch = batch?;
            reused_buffer.clear();
            reused_buffer.resize(batch.num_rows(), 0);
            let batch: DataBlock = batch;
            let col_values = on
                .iter()
                .map(|col_info| batch.column(col_info.index()).clone())
                .collect::<Vec<_>>();

            let indexes = hash_to_buckets(
                &col_values,
                &RandomState::with_seed(partition_infos.level),
                &mut reused_buffer,
                c.bucket_size,
            )?;

            // let mut hash_result: Vec<DataBlock> =
            //     vec![DataBlock::new_empty(batch.schema(), c.bucket_size)];

            let mut rename_later = vec![vec![]; c.bucket_size];
            for (row, bucket_idx) in indexes.iter().enumerate() {
                rename_later[*bucket_idx].push(row);
            }
            // Itertools::fold_ok(&mut self, start, f)
            let maybe_hashed: ArrowResult<Vec<DataBlock>> = rename_later
                .iter()
                .map(|indexes| batch.project(indexes))
                .collect();

            let hash_result = maybe_hashed?;

            for (partition_idx, same_buckets) in hash_result.into_iter().enumerate() {
                let bucket_length = same_buckets.num_rows();

                queuer.enqueue(partition_idx, same_buckets).await?;

                // only care about inner input, we only need to build hashtable from inner input
                if is_inner {
                    match partition_infos.map.get_mut(&partition_idx) {
                        None => {
                            partition_infos.map.insert(
                                partition_idx,
                                PInfo {
                                    memsize: bucket_length,
                                    parent_size: 0,
                                },
                            );
                        }
                        Some(info) => {
                            info.memsize += bucket_length;
                        }
                    };
                }
            }
        }
        Ok(())
    }

    fn _find_next_fallback_partition(
        &mut self,
    ) -> Option<(usize, Arc<dyn PartitionedQueue>, Arc<dyn PartitionedQueue>)> {
        let partition_infos = self.stack.last_mut().unwrap();
        if partition_infos.fallback_partitions.len() == 0 {
            return None;
        }
        let p = partition_infos.fallback_partitions.pop().unwrap();
        return Some((
            p,
            partition_infos.outer_queue.clone(),
            partition_infos.inner_queue.clone(),
        ));
    }

    fn _find_next_inmem_sized_partition(
        &mut self,
        max_size_per_partition: usize,
    ) -> Option<(
        usize,
        usize,
        Arc<dyn PartitionedQueue>,
        Arc<dyn PartitionedQueue>,
    )> {
        let mut found_index = None;
        let partition_infos = self.stack.last_mut().unwrap();
        for (index, item) in partition_infos.map.iter_mut() {
            if item.memsize <= max_size_per_partition {
                found_index = Some(*index);
                break;
            }
        }
        match found_index {
            None => None,
            Some(index) => Some((
                index,
                partition_infos.level,
                partition_infos.outer_queue.clone(),
                partition_infos.inner_queue.clone(),
            )),
        }
    }

    fn _new_hash_joiner(
        &self,
        p_index: usize,
        cur_level: usize,
        schema: SchemaRef,
        outer_queue: Arc<dyn PartitionedQueue>,
        inner_queue: Arc<dyn PartitionedQueue>,
    ) -> HashJoiner {
        let st = HashJoiner::new(
            self.config.on_left.to_owned(),
            self.config.on_right.to_owned(),
            outer_queue,
            inner_queue,
            self.schema,
            p_index,
            RandomState::with_seed(cur_level),
            self.config.batch_size,
            self.join_column_indices.clone(),
        );
        return st;
    }

    async fn recursive_partition(
        queue_allocator: &F,
        config: Config<'a>,
        current_partition: &mut PartitionLevel,
    ) -> SqlResult<Option<PartitionLevel>> {
        let mut ret = None;
        let mut item_remove = -1;

        // inside the map now are large partition that needs recursive
        for (parent_p_index, parinfo) in current_partition.map.iter_mut() {
            let child_partitions = HashMap::new();

            let new_outer_queue = queue_allocator();
            let new_inner_queue = queue_allocator();
            let mut new_level = PartitionLevel {
                fallback_partitions: vec![],
                map: child_partitions,
                inner_queue: new_inner_queue,
                outer_queue: new_outer_queue,
                level: current_partition.level + 1,
            };
            // stream of temporary ouput we have hashed in previous steps
            let mut outer_stream = current_partition
                .outer_queue
                .dequeue(*parent_p_index, config.batch_size)
                .await?;
            Self::partition_batch(&mut outer_stream, &mut new_level, config, false);

            // stream of temporary ouput we have hashed in previous steps
            let mut inner_stream = current_partition
                .inner_queue
                .dequeue(*parent_p_index, config.batch_size)
                .await?;
            Self::partition_batch(&mut inner_stream, &mut new_level, config, true);
            let mut fallbacks = vec![];
            new_level.map.retain(|idx, item| {
                let before_hash = parinfo.memsize as f64;
                let after_hash = item.memsize as f64;
                if before_hash > 0.0 {
                    let size_decrease = 1.0 - (after_hash / before_hash);
                    if size_decrease < 0.05 {
                        fallbacks.push(*idx);
                        return false;
                    }
                }
                return true;
            });
            if fallbacks.len() > 0 {
                new_level.fallback_partitions.extend(fallbacks);
            }

            ret = Some(new_level);
            item_remove = *parent_p_index as i64;
            break;
        }
        if item_remove != -1 {
            current_partition.map.remove(&(item_remove as usize));
        }
        Ok(ret)
    }

    /// We track if there exists a bucket with length > max-size per partition
    fn new(
        c: Config,
        mut left: Box<dyn Operator>,
        mut right: Box<dyn Operator>,
        queue_allocator: F,
        join_column_indices: Vec<ColumnIndex>,
        schema: SchemaRef,
    ) -> SqlResult<Self> {
        let outer_queue = queue_allocator();
        let inner_queue = queue_allocator();
        let mut joiner = GraceHashJoiner {
            schema,
            config: c,
            stack: Vec::new(),
            undone_joining_partition: None,
            queue_allocator,
            join_column_indices,
        };

        let map = HashMap::new();
        let mut first_level_partitions = PartitionLevel {
            fallback_partitions: vec![],
            map,
            outer_queue,
            inner_queue,
            level: 0,
        };
        // TODO: move this to the first next() call
        'until_drain_all: loop {
            let left_partial = left.next()?;
            if left_partial.done {
                let right_partial = right.next()?;
                if right_partial.done {
                    break 'until_drain_all;
                }
                Self::partition_batch(
                    right_partial.inner,
                    &mut first_level_partitions,
                    joiner.config,
                    true,
                );
                // maybe right not done yet
                continue;
            }
            Self::partition_batch(
                left_partial.inner,
                &mut first_level_partitions,
                joiner.config,
                false,
            );
            let right_partial = right.next()?;
            if right_partial.done {
                // maybe left may not done yet
                continue;
            }
            Self::partition_batch(
                right_partial.inner,
                &mut first_level_partitions,
                joiner.config,
                true,
            );
        }

        joiner.stack.push(first_level_partitions);
        return Ok(joiner);
    }
}

// #[cfg(test)]
/* pub mod tests {
    use super::{Config, GraceHashJoiner, HashJoiner, PartitionedQueue};
    use crate::sql::{
        join::{
            grace::Batch,
            queue::{Inmem, MemoryAllocator},
        },
        util::RawInput,
    };
    use core::cell::RefCell;
    use itertools::Itertools;
    use std::{
        cmp::Ordering::{self, Equal},
        sync::Arc,
    };
    use zerocopy::{AsBytes, FromBytes};

    fn make_i64s_row(item: (i64, &[u8])) -> Row {
        let mut vec = Vec::new();
        vec.extend(item.0.to_le_bytes());
        vec.extend(item.1);
        Row::new(vec)
    }

    macro_rules! make_rows {
        // Base case:
        ($a:expr,$b:expr) => {{
            vec![($a, $b)]
        }};
        ($a:expr,$b:expr,$($rest:expr),*) => {{
            [vec![($a,$b)],make_rows!($($rest),*)].concat()
        }};
    }

    struct RowType(i64, Vec<u8>);
    #[test]
    fn test_grace_hash_joiner() {
        struct TestCase {
            outer: Vec<(i64, &'static str)>,
            inner: Vec<(i64, &'static str)>,
            expect: Vec<(i64, &'static str)>,
        }
        // we expect the inner to be hashed, so we can't guarantee order of the rows returned
        let tcases = vec![
            TestCase {
                outer: make_rows!(1, "a1", 2, "b1", 1, "c1"),
                inner: make_rows!(1, "a2", 2, "b2"),
                expect: make_rows!(1, "a1a2", 2, "b1b2", 1, "c1a2"),
            },
            TestCase {
                outer: make_rows!(1, "a1", 2, "b1", 1, "c1"),
                inner: make_rows!(1, "a2", 2, "b2", 1, "a3", 3, "c2"),
                expect: make_rows!(1, "a1a2", 2, "b1b2", 1, "c1a2", 1, "a1a3", 1, "c1a3"),
            },
            TestCase {
                outer: make_rows!(1, "a1", 2, "b1", 1, "c1"),
                inner: make_rows!(1, "a2", 2, "b2", 1, "a3", 3, "c2", 3, "c2", 2, "b2"),
                expect: make_rows!(
                    1, "a1a2", 2, "b1b2", 1, "c1a2", 1, "a1a3", 1, "c1a3", 2, "b1b2"
                ),
            },
        ];
        for item in &tcases {
            let batch_size = 2;
            let left_key_offset = 8; // first 8 bytes represents join key
            let right_key_offset = 8;

            let mut outer_batches = Vec::new();
            for chunk in &item.outer.iter().chunks(batch_size) {
                let mut rows = Vec::new();
                for item in chunk {
                    rows.push(make_i64s_row((item.0, item.1.as_bytes())));
                }
                outer_batches.push(Batch::new(rows));
            }
            let mut inner_batches = Vec::new();
            for chunk in &item.inner.iter().chunks(batch_size) {
                let mut rows = Vec::new();
                for item in chunk {
                    rows.push(make_i64s_row((item.0, item.1.as_bytes())));
                }
                inner_batches.push(Batch::new(rows));
            }
            let alloc = RefCell::new(MemoryAllocator::new());

            let config = Config {
                bucket_size: 2,
                max_size_per_partition: 2,
                batch_size: 2,
            };
            let mut joiner = GraceHashJoiner::new(
                config,
                Box::new(RawInput::new_from_batch(outer_batches)),
                Box::new(RawInput::new_from_batch(inner_batches)),
                || -> Arc<dyn PartitionedQueue> { alloc.borrow_mut().alloc() },
            )
            .expect("creating gracehashjoiner");
            let mut ret: Vec<(i64, Vec<u8>)> = Vec::new();
            // joined result should be 1,1|1,1|1,1|1,1
            while let Some(b) = joiner.next_batch() {
                for row in b.data() {
                    let joined_key = FromBytes::read_from(&row.inner[..8]).unwrap();
                    let other_data = row.inner[8..].to_vec();
                    // let other_data = FromBytes::read_from(&row.inner[8..]).unwrap();
                    ret.push((joined_key, other_data));
                }
            }
            // let expect = [[1, 1], [1, 1], [1, 1], [1, 1]];
            ret.sort_by(|a, b| cmp_row(a, b));
            let mut expect: Vec<(i64, Vec<u8>)> = item
                .expect
                .iter()
                .map(|item| (item.0, item.1.as_bytes().to_vec()))
                .collect();
            expect.sort_by(|a, b| cmp_row(a, b));

            assert_eq!(expect.len(), ret.len(), "wrong number of rows returned");
            let equal = expect.iter().zip(ret.iter()).all(|(expect, real)| {
                compare_row(expect.1.as_bytes().iter(), real.1.as_bytes().iter())
            });
            assert!(equal);
        }
    }
    fn cmp_row<A>(a: &(A, Vec<u8>), b: &(A, Vec<u8>)) -> Ordering
    where
        A: Ord,
    {
        if a.0 != b.0 {
            return a.0.cmp(&b.0);
        }
        assert_eq!(a.1.len(), b.1.len());
        for i in 0..a.1.len() {
            if a.1[i] != b.1[i] {
                return a.1[i].cmp(&b.1[i]);
            }
        }
        Equal
    }

    #[test]
    fn test_inmem_joiner() {
        struct TestCase {
            outer: Vec<i64>,
            inner: Vec<i64>,
            expect: Vec<i64>,
        }
        let tcases = vec![
            TestCase {
                outer: vec![1, 1, 1, 1],
                inner: vec![1, 2, 2, 2],
                expect: vec![1, 1, 1, 1],
            },
            TestCase {
                outer: vec![1, 2, 1, 2],
                inner: vec![2, 1],
                expect: vec![1, 2, 1, 2],
            },
        ];
        for item in &tcases {
            let p_index = 1;
            let batch_size = 2;
            let left_key_offset = 8; // first 8 bytes represents join key
            let right_key_offset = 8;

            let mut outer_rows = Vec::new();
            for i in &item.outer {
                outer_rows.push(make_i64s_row((*i, &i.to_le_bytes()[..])));
            }
            let mut inner_rows = Vec::new();
            for i in &item.inner {
                inner_rows.push(make_i64s_row((*i, &i.to_le_bytes()[..])));
            }

            let in_queue = Inmem::new(1);
            let out_queue = Inmem::new(2);
            in_queue.enqueue(1, inner_rows);
            out_queue.enqueue(1, outer_rows);
            let mut joiner = HashJoiner::new(
                Arc::new(out_queue),
                Arc::new(in_queue),
                p_index,
                batch_size,
                left_key_offset,
                right_key_offset,
            );
            let mut ret: Vec<i64> = Vec::new();
            // joined result should be 1,1|1,1|1,1|1,1
            while let Some(b) = joiner.next() {
                for row in b.data() {
                    let joined_key = FromBytes::read_from(&row.inner[..8]).unwrap();
                    ret.push(joined_key);
                }
            }

            assert_eq!(
                item.expect.len(),
                ret.len(),
                "wrong number of rows returned"
            );
            let equal = item
                .expect
                .iter()
                .zip(ret.iter())
                .all(|(real, expect)| real == expect);
            assert!(equal);
        }
    }

    fn compare_row<'a>(
        left: impl Iterator<Item = &'a u8>,
        right: impl Iterator<Item = &'a u8>,
    ) -> bool {
        left.zip(right).all(|(a, b)| a == b)
    }
} */
