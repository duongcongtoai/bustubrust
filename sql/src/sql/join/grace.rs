use super::inmem::HashJoinOp;
use crate::sql::{
    exe::{BoxedDataIter, DataIter, Executor, Operator, PlanType, SchemaDataIter},
    join::hash_util::hash_to_buckets,
    util::GeneratorIteratorAdapter,
    DataBlock, ExecutionContext, SqlResult,
};

use ahash::RandomState;
use datafusion::{
    arrow::{
        array::{Array, PrimitiveArray},
        compute::kernels::take::take,
        datatypes::{Schema, SchemaRef, UInt64Type},
    },
    physical_plan::{
        expressions::Column,
        join_utils::{ColumnIndex, JoinSide},
    },
};
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

impl Operator for GraceHashJoinOp {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Before return, it enqueue all data from left and right input to disk spillable queue
    fn execute_sync(&mut self, ctx: ExecutionContext) -> SqlResult<BoxedDataIter> {
        let outer_queue = ctx.new_queue(self.config.outer_schema.clone());
        let inner_queue = ctx.new_queue(self.config.inner_schema.clone());

        println!("addr of outer_queue {:p}", outer_queue);
        let map = HashMap::new();
        let mut first_level_partitions = PartitionLevel {
            fallback_partitions: vec![],
            map,
            outer_queue,
            inner_queue,
            level: 0,
        };
        let mut left_stream = self.left_op.execute_sync(ctx.clone())?;
        let mut right_stream = self.right_op.execute_sync(ctx.clone())?;
        Self::partition_batch(
            &mut right_stream,
            &mut first_level_partitions,
            &self.config,
            true,
        )?;

        Self::partition_batch(
            &mut left_stream,
            &mut first_level_partitions,
            &self.config,
            false,
        )?;

        let mut stack: Vec<PartitionLevel> = vec![];
        stack.push(first_level_partitions);
        let moved_ctx = ctx.clone();
        let schema = self.schema.clone();
        // let schema_cloned = self.schema.clone();
        let config = self.config.clone();
        let joined_column_indices = self.join_column_indices.clone();

        let gen = move || {
            'recursiveloop: while stack.len() > 0 {
                let config = config.clone();
                while let Some((p_index, cur_level, outer_queue, inner_queue)) =
                    Self::_find_next_inmem_sized_partition(
                        &mut stack,
                        config.max_size_per_partition,
                    )
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
                    // let mut inmem_stream = inmem_joiner.execute_sync(moved_ctx.clone())?;
                    /* let (tx, mut rx) = tokio::sync::mpsc::channel(1);
                    let moved_ctx2 = moved_ctx.clone();
                    tokio::spawn(async move{
                        let  inmem_stream = inmem_joiner.execute(moved_ctx2).await;
                        match tx.send(inmem_stream).await{
                            Err(some_err) => panic!(some_err),
                            Ok(_) => {},
                        }
                    }); */

                    let mut inmem_stream = inmem_joiner.execute_sync(moved_ctx.clone()).unwrap();
                    while let Some(batch_ret) = inmem_stream.next() {
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
                            .unwrap()
                    {
                        stack.push(next_recursive_p);
                        continue 'recursiveloop;
                    }
                }
                stack.pop();
            }
        };
        let st2 = GeneratorIteratorAdapter::new(gen);
        Ok(SchemaDataIter::new(self.schema.clone(), Box::new(st2)))
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

pub trait PartitionedQueue: Sync + Send + Debug {
    fn enqueue(&self, partition_idx: usize, data: DataBlock) -> SqlResult<()>;

    fn dequeue_all(&self, partition_idx: usize) -> SqlResult<DataBlock>;

    fn dequeue(&self, partition_idx: usize, size: usize) -> SqlResult<BoxedDataIter>;

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
            column_indices,
            Arc::new(schema),
        )
        .expect("failted to create grace hash joiner");
    }

    fn partition_batch<'b>(
        batch_stream: &'b mut BoxedDataIter,
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
        while let Some(batch) = batch_stream.next() {
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

            let mut buckets = vec![vec![]; c.bucket_size];
            for (row, bucket_idx) in reused_buffer.iter().enumerate() {
                buckets[*bucket_idx as usize].push(row as u64);
            }

            for (bucket_idx, row_indices) in buckets.into_iter().enumerate() {
                let indices: PrimitiveArray<UInt64Type> = row_indices.into();
                let columns = batch
                    .columns()
                    .iter()
                    .map(|c| take(c.as_ref(), &indices, None).map_err(|e| e.into()))
                    .collect::<SqlResult<Vec<Arc<dyn Array>>>>()?;

                let output_batch = DataBlock::try_new(batch.schema(), columns)?;

                let bucket_length = output_batch.num_rows();
                queuer.enqueue(bucket_idx, output_batch).unwrap();

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
            Some(index) => {
                partition_infos.map.remove(&index).unwrap();
                Some((
                    index,
                    partition_infos.level,
                    partition_infos.outer_queue.clone(),
                    partition_infos.inner_queue.clone(),
                ))
            }
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

    fn recursive_partition(
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
            Self::partition_batch(&mut outer_stream, &mut new_level, &config, false)?;

            // stream of temporary ouput we have hashed in previous steps
            let mut inner_stream = current_partition
                .inner_queue
                .dequeue(*parent_p_index, batch_size)?;
            Self::partition_batch(&mut inner_stream, &mut new_level, &config, true)?;
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
    use super::{Config, GraceHashJoinOp, HashJoinOp, PartitionedQueue};
    use crate::sql::{
        exe::{ExecutionContext, Operator},
        inmem_op::InMemOp,
        join::queue::Inmem,
        util::collect,
        DataBlock,
    };
    use ahash::RandomState;
    use datafusion::{
        arrow::{
            array::{Array, Int32Array},
            datatypes::{DataType, Field, Schema},
        },
        physical_plan::expressions::Column,
    };
    use std::sync::Arc;

    struct RowType(i64, Vec<u8>);
    fn build_i32_table_box(cols_and_values: Vec<(&str, Vec<i32>)>) -> Box<dyn Operator> {
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
        Box::new(InMemOp::new(schema, vec![batch]))
    }

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

    fn collect_batch_from_op(mut op: Arc<dyn Operator>, ctx: ExecutionContext) -> Vec<DataBlock> {
        let outer_input_stream = Arc::get_mut(&mut op)
            .unwrap()
            .execute_sync(ctx.clone())
            .expect("failed to execute operator");
        collect(outer_input_stream).expect("failed to collect output")
    }

    #[test]
    fn test_inmem_joiner() {
        let outer = build_i32_table(vec![("col_a", vec![1, 2, 3]), ("col_b", vec![2, 3, 4])]);
        let inner = build_i32_table(vec![
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
        let outer_batches = collect_batch_from_op(outer, ctx.clone());
        let inner_batches = collect_batch_from_op(inner, ctx.clone());

        for item in outer_batches {
            out_queue.enqueue(1, item).unwrap();
        }

        for item in inner_batches {
            in_queue.enqueue(1, item).unwrap();
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
            .execute_sync(ctx.clone())
            .expect("executing join op");
        let batches = collect(stream).expect("failed to collect from joined stream");
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

    #[test]
    fn test_grace_hash_joiner() {
        let outer = build_i32_table_box(vec![("col_a", vec![1, 2, 3]), ("col_b", vec![2, 3, 4])]);
        let inner = build_i32_table_box(vec![
            ("col_a", vec![1, 4, 3, 5]),
            ("col_c", vec![2, 3, 4, 1]),
        ]);
        let on_left = vec![Column::new("col_a", 0)];
        let on_right = vec![Column::new("col_a", 0)];
        let conf = Config {
            bucket_size: 10,
            max_size_per_partition: 2,
            batch_size: 2,
            on_left,
            on_right,
            outer_schema: outer.schema(),
            inner_schema: inner.schema(),
        };
        let (combined_schema, joined_column_indices) =
            super::build_join_schema(&outer.schema(), &inner.schema());
        let ctx = ExecutionContext::new_for_test();

        let mut hj = GraceHashJoinOp::new(
            conf,
            outer,
            inner,
            joined_column_indices,
            Arc::new(combined_schema),
        )
        .expect("failted to create grace hash joiner");
        let stream = hj.execute_sync(ctx.clone()).expect("executing join op");
        let batches = collect(stream).expect("failed to collect from joined stream");
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
}
