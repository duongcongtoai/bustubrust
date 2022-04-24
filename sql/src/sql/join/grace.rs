use super::inmem::{HashJoinOp, HashJoiner};
use crate::sql::{
    exe::{
        DataBlockStream, Executor, Operator, PlanType, SchemaStream, SendableDataBlockStream,
        SendableResult,
    },
    join::hash_util::hash_to_buckets,
    DataBlock, ExecutionContext, SqlResult,
};
use ahash::RandomState;
use async_stream::{stream, try_stream};
use datafusion::{
    arrow::{
        array::Array,
        datatypes::{DataType, Schema, SchemaRef},
        error::Result as ArrowResult,
    },
    physical_plan::{
        expressions::Column,
        join_utils::{ColumnIndex, JoinSide},
    },
};
use futures::{Future, StreamExt};
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
pub struct GraceHashJoinOp {
    config: Config,
    stack: Vec<PartitionLevel>,
    left_op: Box<dyn Operator>,
    right_op: Box<dyn Operator>,
    join_column_indices: Vec<ColumnIndex>,
    schema: SchemaRef,
}
unsafe impl Send for GraceHashJoinOp {}
unsafe impl Sync for GraceHashJoinOp {}

pub struct GraceHashJoiner {
    ctx: ExecutionContext,
    config: Config,
    stack: Vec<PartitionLevel>,
    join_column_indices: Vec<ColumnIndex>,
    schema: SchemaRef,
}

#[async_trait::async_trait]
impl Operator for GraceHashJoinOp {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Before return, it enqueue all data from left and right input to disk spillable queue
    async fn execute(&mut self, ctx: ExecutionContext) -> SqlResult<SendableDataBlockStream> {
        let outer_queue = ctx.new_queue(self.config.outer_schema.clone());
        let inner_queue = ctx.new_queue(self.config.inner_schema.clone());
        let map = HashMap::new();
        let mut first_level_partitions = PartitionLevel {
            fallback_partitions: vec![],
            map,
            outer_queue,
            inner_queue,
            level: 0,
        };
        let mut left_stream = self.left_op.execute(ctx.clone()).await?;
        let mut right_stream = self.right_op.execute(ctx.clone()).await?;
        Self::partition_batch(
            &mut right_stream,
            &mut first_level_partitions,
            &self.config,
            true,
        )
        .await;

        Self::partition_batch(
            &mut left_stream,
            &mut first_level_partitions,
            &self.config,
            false,
        )
        .await;

        let mut stack: Vec<PartitionLevel> = vec![];
        stack.push(first_level_partitions);
        let moved_ctx = ctx.clone();
        let schema = self.schema.clone();
        let config = self.config.clone();
        let joined_column_indices = self.join_column_indices.clone();

        Ok(SchemaStream::new(
            schema.clone(),
            Box::pin(stream! {
                'recursiveloop: while stack.len() > 0 {
                    let config = config.clone();
                    while let Some((p_index, cur_level, outer_queue, inner_queue)) =
                        Self::_find_next_inmem_sized_partition(&mut stack,config.max_size_per_partition)
                    {
                        let mut inmem_joiner = Self::_new_hash_joiner(
                            &config,
                            schema.clone(),
                            joined_column_indices.clone(),
                            p_index,
                            cur_level,
                            outer_queue,
                            inner_queue,
                        );

                        // I used execute_sync because awaiting for a future inside this macro
                        // requires the future to be sync, i don't know how to do that yet
                        let mut inmem_stream = inmem_joiner.execute_sync(moved_ctx.clone())?;
                        while let Some(batch_ret) = inmem_stream.next().await {
                            yield batch_ret;
                        }
                    }
                    while let Some((p_index, outer_queue, inner_queue)) =
                        Self::_find_next_fallback_partition(&mut stack)
                    {
                        // p_index is a partition that even if partition one more time, it may not fit in
                        // memory, so we use fallback strategy like merge join instead
                        // make this fallback operation as late as possible
                        panic!("unimplemented")
                    }

                    let cur_partitions = stack.last_mut().unwrap();

                    // recursive partition
                    if cur_partitions.map.len() > 0 {
                        // let st = cur_partitions.last_mut();
                        if let Some(next_recursive_p) =
                            Self::recursive_partition(moved_ctx.clone(), config, cur_partitions)
                                .await?
                        {
                            stack.push(next_recursive_p);
                            continue 'recursiveloop;
                        }
                    }
                    stack.pop();
                }
            }),
        ))
    }

    fn execute_sync(&mut self, _: ExecutionContext) -> SqlResult<SendableDataBlockStream> {
        todo!()
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
    // async fn enqueue(&self, partition_idx: usize, data: DataBlock) -> SqlResult<()>;

    fn enqueue(&self, partition_idx: usize, data: DataBlock) -> SendableResult;

    async fn dequeue_all(&self, partition_idx: usize) -> SqlResult<DataBlock>;
    // fn dequeue(&self, partition_idx: usize) -> SqlResult<SendableDataBlockStream>;

    fn dequeue(&self, partition_idx: usize, size: usize) -> SqlResult<SendableDataBlockStream>;
    fn id(&self) -> usize;
}

#[derive(Debug)]
struct PInfo {
    parent_size: usize,
    memsize: usize,
}

#[derive(Clone, Debug)]
struct Config {
    bucket_size: usize,
    max_size_per_partition: usize,
    batch_size: usize,
    left_key_offset: usize,
    on_left: Vec<Column>,
    on_right: Vec<Column>,
    outer_schema: SchemaRef,
    inner_schema: SchemaRef,
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

impl GraceHashJoinOp {
    /// A big todo here
    pub fn from_plan(plan: GraceHashJoinPlan, ctx: ExecutionContext) -> Self {
        let left_op = Executor::create_from_subplan_operator(*plan.left_plan, ctx.clone());
        let right_op = Executor::create_from_subplan_operator(*plan.right_plan, ctx.clone());
        let default_config = Config {
            bucket_size: 10,
            max_size_per_partition: 10,
            batch_size: 10,
            left_key_offset: 4,
            on_left: plan.on_left.clone(),
            on_right: plan.on_right.clone(),
            outer_schema: left_op.schema(),
            inner_schema: right_op.schema(),
        };
        let (left_schema, right_schema) = (left_op.schema(), right_op.schema());

        let (schema, column_indices) = build_join_schema(&left_schema, &right_schema);

        return GraceHashJoinOp::new(
            default_config,
            left_op,
            right_op,
            // move || -> Arc<dyn PartitionedQueue> { ctx.new_queue() },
            column_indices,
            Arc::new(schema),
        )
        .expect("failted to create grace hash joiner");
    }

    async fn partition_batch<'b>(
        batch_stream: &'b mut SendableDataBlockStream,
        partition_infos: &mut PartitionLevel,
        c: &Config,
        is_inner: bool,
    ) -> SqlResult<()> {
        let mut on = &c.on_left;
        if is_inner {
            on = &c.on_right;
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

            hash_to_buckets(
                &col_values,
                &RandomState::with_seed(partition_infos.level),
                &mut reused_buffer,
                c.bucket_size,
            )?;

            // let mut hash_result: Vec<DataBlock> =
            //     vec![DataBlock::new_empty(batch.schema(), c.bucket_size)];

            //
            let mut buckets = vec![vec![]; c.bucket_size];
            for (bucket_idx, row) in reused_buffer.iter().enumerate() {
                buckets[bucket_idx].push(*row as usize);
            }
            let data_by_buckets: ArrowResult<Vec<DataBlock>> = buckets
                .iter()
                .map(|same_bucket_rows| batch.project(same_bucket_rows.as_slice()))
                .collect();

            let data_by_buckets = data_by_buckets?;

            for (bucket_idx, same_buckets) in data_by_buckets.into_iter().enumerate() {
                let bucket_length = same_buckets.num_rows();

                queuer.enqueue(bucket_idx, same_buckets).await?;

                // only care about inner input, we only need to build hashtable from inner input
                if is_inner {
                    match partition_infos.map.get_mut(&bucket_idx) {
                        None => {
                            partition_infos.map.insert(
                                bucket_idx,
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
        stack: &mut Vec<PartitionLevel>,
    ) -> Option<(usize, Arc<dyn PartitionedQueue>, Arc<dyn PartitionedQueue>)> {
        let partition_infos = stack.last_mut().unwrap();
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
        stack: &mut Vec<PartitionLevel>,
        max_size_per_partition: usize,
    ) -> Option<(
        usize,
        usize,
        Arc<dyn PartitionedQueue>,
        Arc<dyn PartitionedQueue>,
    )> {
        let mut found_index = None;
        let partition_infos = stack.last_mut().unwrap();
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
        config: &Config,
        schema: SchemaRef,
        joined_column_indices: Vec<ColumnIndex>,
        p_index: usize,
        cur_level: usize,
        outer_queue: Arc<dyn PartitionedQueue>,
        inner_queue: Arc<dyn PartitionedQueue>,
    ) -> HashJoinOp {
        let st = HashJoinOp::new(
            config.on_left.to_owned(),
            config.on_right.to_owned(),
            outer_queue,
            inner_queue,
            schema,
            p_index,
            RandomState::with_seed(cur_level),
            config.batch_size,
            joined_column_indices.clone(),
        );
        return st;
    }

    async fn recursive_partition(
        ctx: ExecutionContext,
        config: Config,
        current_partition: &mut PartitionLevel,
    ) -> SqlResult<Option<PartitionLevel>> {
        let mut ret = None;
        let mut item_remove = -1;
        let batch_size = config.batch_size;

        // inside the map now are large partition that needs recursive
        for (parent_p_index, parinfo) in current_partition.map.iter_mut() {
            let child_partitions = HashMap::new();

            let new_outer_queue = ctx.new_queue(config.outer_schema.clone());
            let new_inner_queue = ctx.new_queue(config.inner_schema.clone());
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
                .dequeue(*parent_p_index, batch_size)?;
            Self::partition_batch(&mut outer_stream, &mut new_level, &config, false).await?;

            // stream of temporary ouput we have hashed in previous steps
            let mut inner_stream = current_partition
                .inner_queue
                .dequeue(*parent_p_index, batch_size)?;
            Self::partition_batch(&mut inner_stream, &mut new_level, &config, true).await?;
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
        left_op: Box<dyn Operator>,
        right_op: Box<dyn Operator>,
        join_column_indices: Vec<ColumnIndex>,
        schema: SchemaRef,
    ) -> SqlResult<Self> {
        let joiner = GraceHashJoinOp {
            schema,
            config: c,
            stack: Vec::new(),
            left_op,
            right_op,
            // queue_allocator,
            join_column_indices,
        };

        return Ok(joiner);
    }
}

#[cfg(test)]
pub mod tests {
    use super::{Config, GraceHashJoiner, HashJoinOp, HashJoiner, PartitionedQueue};
    use crate::sql::{
        exe::ExecutionContext,
        inmem_op::InMemOp,
        join::{
            grace::Operator,
            queue::{Inmem, MemoryAllocator},
        },
        util::{collect, RawInput},
        DataBlock,
    };
    use ahash::RandomState;
    use core::cell::RefCell;
    use datafusion::{
        arrow::{
            array::{Array, Int32Array},
            datatypes::{
                DataType::{self, Int8},
                Field, Schema, SchemaRef,
            },
        },
        physical_plan::expressions::Column,
    };
    use itertools::Itertools;
    use std::{
        cmp::Ordering::{self, Equal},
        sync::Arc,
    };
    use tokio::task;
    use zerocopy::{AsBytes, FromBytes};

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

    fn build_i32_table(cols_and_values: Vec<(&str, Vec<i32>)>) -> Arc<dyn Operator> {
        let field_vec = cols_and_values
            .iter()
            .map(|(col_name, _)| Field::new(col_name, DataType::Int32, false))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(field_vec));
        let batches = cols_and_values
            .iter()
            .map(|(_, values_for_this_cols)| {
                Arc::new(Int32Array::from(values_for_this_cols.clone())) as Arc<dyn Array>
            })
            .collect::<Vec<_>>();
        let batch: DataBlock = DataBlock::try_new(schema.clone(), batches).unwrap();
        Arc::new(InMemOp::new(schema, vec![batch]))
    }

    #[tokio::test]
    async fn test_inmem_joiner() {
        let mut outer = build_i32_table(vec![("col_a", vec![1, 2, 3]), ("col_b", vec![2, 3, 4])]);
        let mut inner = build_i32_table(vec![
            ("col_a", vec![1, 4, 3, 5]),
            ("col_c", vec![2, 3, 4, 1]),
        ]);
        let on_left = vec![Column::new("col_a", 0)];
        let on_right = vec![Column::new("col_a", 0)];

        let in_queue = Inmem::new(1, inner.schema());
        let out_queue = Inmem::new(2, outer.schema());
        let (combined_schema, joined_column_indices) =
            super::build_join_schema(&outer.schema(), &inner.schema());

        let ctx = ExecutionContext::new_for_test();
        let outer_input_stream = Arc::get_mut(&mut outer)
            .unwrap()
            .execute(ctx.clone())
            .await
            .expect("aa");
        let outer_data_block = collect(outer_input_stream).await.expect("bb");
        for item in outer_data_block {
            out_queue.enqueue(1, item);
        }

        let inner_input_stream = Arc::get_mut(&mut inner)
            .unwrap()
            .execute(ctx.clone())
            .await
            .expect("aa");
        let inner_data_block = collect(inner_input_stream).await.expect("bb");
        for item in inner_data_block {
            in_queue.enqueue(1, item);
        }

        let random_state = RandomState::new();
        let mut joined_op = HashJoinOp::new(
            on_left,
            on_right,
            Arc::new(out_queue),
            Arc::new(in_queue),
            Arc::new(combined_schema),
            1,
            random_state,
            10,
            joined_column_indices,
        );
        let stream = joined_op
            .execute(ctx.clone())
            .await
            .expect("executing join op");
        let batches = collect(stream)
            .await
            .expect("failed to collect from joined stream");
        let expected = vec![
            "+-------+-------+-------+-------+",
            "| col_a | col_b | col_a | col_c |",
            "+-------+-------+-------+-------+",
            "| 1     | 2     | 1     | 2     |",
            "| 3     | 4     | 3     | 4     |",
            "+-------+-------+-------+-------+",
        ];
        crate::assert_batches_sorted_eq!(expected, &batches);
    }

    /* fn test_grace_hash_joiner() {
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
    } */
}
