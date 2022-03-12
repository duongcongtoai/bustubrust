use libc::key_t;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::rc::Rc;
use twox_hash::xxh3::hash64;
use zerocopy::FromBytes;

struct CachePool {}

impl CachePool {
    fn next() {}
}
struct HashJoiner {
    outer_queue: Rc<dyn PartitionedQueue>,
    inner_queue: Rc<dyn PartitionedQueue>,
    p_index: usize,
    batch_size: usize,
    built: bool,
    htable: HashMap<Vec<u8>, Vec<Row>>,
    left_key_offset: usize,
    right_key_offset: usize,
}
impl HashJoiner {
    pub fn new(
        outer_queue: Rc<dyn PartitionedQueue>,
        inner_queue: Rc<dyn PartitionedQueue>,
        p_index: usize,
        batch_size: usize,
        left_key_offset: usize,
        right_key_offset: usize,
    ) -> Self {
        HashJoiner {
            outer_queue,
            inner_queue,
            p_index,
            batch_size,
            left_key_offset,
            right_key_offset,
            built: false,
            htable: HashMap::new(),
        }
    }
}

struct GraceHashJoiner<F>
where
    F: Fn() -> Rc<dyn PartitionedQueue>,
{
    /* partition_queue_inner: Rc<dyn PartitionedQueue>,
    partition_queue_outer: Rc<dyn PartitionedQueue>, */
    config: Config,
    stack: Vec<PartitionLevel>,
    undone_joining_partition: Option<HashJoiner>,
    queue_allocator: F,
}
struct PartitionLevel {
    map: HashMap<usize, PInfo>,
    outer_queue: Rc<dyn PartitionedQueue>,
    inner_queue: Rc<dyn PartitionedQueue>,
}

/* trait PartitionAllocator {
    fn new() ->

} */

pub trait PartitionedQueue {
    fn enqueue(&self, partition_idx: usize, data: Vec<Row>);
    fn dequeue(&self, partition_idx: usize, size: usize) -> Option<Batch>;
}

// struct RcDynPartitionQueue(Rc<dyn PartitionedQueue>);

struct PInfo {
    parent_size: usize,
    memsize: usize,
}
pub struct Batch {
    inner: Vec<Row>,
}
impl Batch {
    pub fn new(r: Vec<Row>) -> Self {
        Batch { inner: r }
    }
    pub fn data(&self) -> &Vec<Row> {
        &self.inner
    }
}

#[derive(Clone, Debug)]
pub struct Row {
    pub inner: Vec<u8>,
}
impl Row {
    fn new(inner: Vec<u8>) -> Self {
        Row { inner }
    }
}

impl HashJoiner {
    /* fn left_joined_key<'a>(self, r: &'a Row) -> &'a [u8] {
        &r.inner[..self.left_key_offset]
    } */
    fn joined_key<'a>(key_offset: usize, r: &'a Row) -> &'a [u8] {
        &r.inner[..key_offset]
    }

    fn _build(&mut self) {
        let htable = &mut self.htable;
        while let Some(batch) = self.inner_queue.dequeue(self.p_index, self.batch_size) {
            for item in batch.inner {
                let key = Self::joined_key(self.right_key_offset, &item);
                match htable.get_mut(key) {
                    Some(datas) => {
                        datas.push(item);
                    }
                    None => {
                        htable.insert(key.to_vec(), vec![item]);
                    }
                }
            }
        }
        self.built = true;
    }

    fn next(&mut self) -> Option<Batch> {
        if !self.built {
            self._build();
        }

        let mut ret = Vec::new();
        while let Some(outer_batch) = self.outer_queue.dequeue(self.p_index, self.batch_size) {
            for item in outer_batch.inner {
                match self
                    .htable
                    .get(Self::joined_key(self.left_key_offset, &item))
                {
                    None => continue,
                    Some(inner_matches) => {
                        for inner_match in inner_matches {
                            let mut joined_row = item.clone();
                            joined_row.inner.extend(&inner_match.inner);
                            ret.push(joined_row);
                            if ret.len() == self.batch_size {
                                return Some(Batch::new(ret));
                            }
                        }
                    }
                }
            }
        }
        return None;
    }
}

#[derive(Copy, Clone)]
struct Config {
    bucket_size: usize,
    max_size_per_partition: usize,
    batch_size: usize,
    left_key_offset: usize,
    right_key_offset: usize,
}

// left: 1,2,3,..,10
// right: 1,1,2,2,...10,10
// initial partition = 2
// max-size per partition = 2
impl<F> GraceHashJoiner<F>
where
    F: Fn() -> Rc<dyn PartitionedQueue>,
{
    fn partition_batch(
        queuer: &Rc<dyn PartitionedQueue>,
        b: Batch,
        partition_infos: &mut HashMap<usize, PInfo>,
        c: Config,
        is_inner: bool,
        // right_key_offset: usize,
    ) {
        /* let mut queuer = &self.partition_queue_outer;
        if input_idx == 1 {
            queuer = &self.partition_queue_inner;
        } */
        let mut hash_result: Vec<Vec<Row>> = Vec::new();
        for _ in 0..c.bucket_size {
            hash_result.push(Vec::new())
        }

        let key_offset = match is_inner {
            true => c.right_key_offset,
            false => c.left_key_offset,
        };

        for item in b.inner {
            let h = Self::hash(Self::get_key(key_offset, &item), c.bucket_size as u64);
            hash_result[h].push(item);
        }
        for (partition_idx, same_buckets) in hash_result.into_iter().enumerate() {
            let bucket_length = same_buckets.len();
            // let partition_idx = self.current_partition_offset + bucket_idx;
            queuer.enqueue(partition_idx, same_buckets);

            // only care about inner input
            if is_inner {
                match partition_infos.get_mut(&partition_idx) {
                    None => {
                        partition_infos.insert(
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

    fn _find_next_inmem_sized_partition(
        &mut self,
        max_size_per_partition: usize,
        // partition_infos: &mut HashMap<usize, PInfo>,
    ) -> Option<(
        usize,
        PInfo,
        Rc<dyn PartitionedQueue>,
        Rc<dyn PartitionedQueue>,
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
                partition_infos.map.remove(&index).unwrap(),
                partition_infos.outer_queue.clone(),
                partition_infos.inner_queue.clone(),
            )),
        }
    }

    fn _new_hash_joiner(
        &self,
        p_index: usize,
        outer_queue: Rc<dyn PartitionedQueue>,
        inner_queue: Rc<dyn PartitionedQueue>,
    ) -> HashJoiner {
        /* let p = self
        .partition_queue_inner
        .dequeue(p_index, self.batch_size)
        .unwrap(); */

        let st = HashJoiner {
            batch_size: self.config.batch_size,
            htable: HashMap::new(),
            outer_queue,
            inner_queue,
            built: false,
            p_index,
            left_key_offset: self.config.left_key_offset,
            right_key_offset: self.config.right_key_offset,
        };
        return st;
    }

    /* fn stack_len(&self) -> usize {
        self.stack.len()
    }
    fn stack_pop(&mut self) {
        self.stack.pop();
    }

    fn pop_partition_len(&mut self) -> usize {
        self.stack.last_mut().unwrap().len()
    }

    fn current_partition(&mut self) -> &mut HashMap<usize, PInfo> {
        self.stack.last_mut().unwrap()
    } */

    fn next(&mut self) -> Option<Batch> {
        if let Some(inmem_joiner) = &mut self.undone_joining_partition {
            match inmem_joiner.next() {
                None => {
                    self.undone_joining_partition = None;
                }
                Some(batch_result) => {
                    return Some(batch_result);
                }
            };
        };

        'recursiveloop: while self.stack.len() > 0 {
            while let Some((p_index, _, outer_queue, inner_queue)) = self
                ._find_next_inmem_sized_partition(
                    self.config.max_size_per_partition,
                    // &mut cur_partitions,
                )
            {
                let mut inmem_joiner = self._new_hash_joiner(p_index, outer_queue, inner_queue);
                match inmem_joiner.next() {
                    None => {
                        self.undone_joining_partition = None;
                        continue;
                    }
                    Some(batch_result) => {
                        self.undone_joining_partition = Some(inmem_joiner);
                        return Some(batch_result);
                    }
                }
            }

            let cur_partitions = self.stack.last_mut().unwrap();

            // recursive partition
            if cur_partitions.map.len() > 0 {
                // let st = cur_partitions.last_mut();
                if let Some(next_recursive_p) =
                    Self::recursive_partition(&self.queue_allocator, self.config, cur_partitions)
                {
                    self.stack.push(next_recursive_p);
                    continue 'recursiveloop;
                }
            }
            self.stack.pop();
        }
        None

        // Find next partition that we can process without having to
        // recursively repartition.
    }
    fn recursive_partition(
        queue_allocator: &F,
        config: Config,
        current_partition: &mut PartitionLevel,
    ) -> Option<PartitionLevel> {
        // let cur_partitions = self.stack.last_mut().unwrap();
        let mut ret = None;
        let mut item_remove = -1;
        for (parent_p_index, _) in current_partition.map.iter_mut() {
            let mut child_partitions = HashMap::new();
            let new_outer_queue = queue_allocator();
            while let Some(batch) = current_partition
                .outer_queue
                .dequeue(*parent_p_index, config.batch_size)
            {
                // this will create partition
                Self::partition_batch(
                    &new_outer_queue,
                    batch,
                    &mut child_partitions,
                    config,
                    false,
                );
            }
            let new_inner_queue = queue_allocator();
            while let Some(batch) = current_partition
                .inner_queue
                .dequeue(*parent_p_index, config.batch_size)
            {
                // this will create partition
                Self::partition_batch(
                    &new_inner_queue,
                    batch,
                    &mut child_partitions,
                    config,
                    false,
                );
            }

            ret = Some(PartitionLevel {
                map: child_partitions,
                inner_queue: new_inner_queue,
                outer_queue: new_outer_queue,
            });
            item_remove = *parent_p_index as i64;
        }
        if item_remove != -1 {
            current_partition.map.remove(&(item_remove as usize));
        }
        ret
    }
    // we track if there exists a bucket with length > max-size per partition
    fn new(
        c: Config,
        mut left: impl Iterator<Item = Batch>,
        mut right: impl Iterator<Item = Batch>,
        queue_allocator: F,
        /* outer_queue: Rc<dyn PartitionedQueue>,
        inner_queue: Rc<dyn PartitionedQueue>, */
    ) -> Self {
        // let hash_result: Vec<Vec<Row>> = Vec::new();
        //
        let outer_queue = queue_allocator();
        let inner_queue = queue_allocator();
        let mut joiner = GraceHashJoiner {
            config: c,
            stack: Vec::new(),
            undone_joining_partition: None,
            queue_allocator,
        };

        // call left.Next() and right.Next()
        // until both return None, do
        /* let maybe_left = left.next();
        let maybe_right = right.next(); */
        let mut first_level_partitions = HashMap::new();
        'until_drain_all: loop {
            match left.next() {
                Some(left_batch) => {
                    Self::partition_batch(
                        &outer_queue,
                        left_batch,
                        &mut first_level_partitions,
                        joiner.config,
                        false,
                    );
                    match right.next() {
                        Some(right_batch) => {
                            Self::partition_batch(
                                &inner_queue,
                                right_batch,
                                &mut first_level_partitions,
                                joiner.config,
                                true,
                            );
                            //left_batch,right_batch
                        }
                        None => {}
                    };
                }
                None => {
                    match right.next() {
                        Some(right_batch) => {
                            Self::partition_batch(
                                &inner_queue,
                                right_batch,
                                &mut first_level_partitions,
                                joiner.config,
                                true,
                            );
                        }
                        None => break 'until_drain_all,
                    };
                }
            };
        }
        let info = PartitionLevel {
            map: first_level_partitions,
            outer_queue,
            inner_queue,
        };

        joiner.stack.push(info);
        return joiner;
    }

    fn hash(bytes: &[u8], bucket_size: u64) -> usize {
        let ret = hash64(bytes);
        return (ret % bucket_size) as usize;
    }
    fn get_key<'a>(key_offset: usize, r: &'a Row) -> &'a [u8] {
        &r.inner[..key_offset]
    }
}

#[cfg(test)]
pub mod tests {
    use super::{Config, GraceHashJoiner, HashJoiner, PartitionedQueue, Row};
    use crate::join::grace::Batch;
    use crate::join::queue::{Inmem, MemoryAllocator};
    use core::cell::RefCell;
    use itertools::Itertools;
    use std::rc::Rc;
    use zerocopy::FromBytes;

    fn make_i64s_row(a: impl IntoIterator<Item = i64>) -> Row {
        let mut vec = Vec::new();
        for item in a {
            vec.extend(item.to_le_bytes());
        }
        Row::new(vec)
    }
    #[test]
    fn test_grace_h_joiner() {
        struct TestCase {
            outer: Vec<i64>,
            inner: Vec<i64>,
            expect: Vec<Vec<i64>>,
        }
        // we expect the inner to be hashed, so we can't guarantee order of the rows returned
        let tcases = vec![
            TestCase {
                outer: vec![1, 1, 1, 1],
                inner: vec![1, 2, 2, 2],
                expect: vec![vec![1, 1], vec![1, 1], vec![1, 1], vec![1, 1]],
            },
            TestCase {
                outer: vec![1, 2, 1, 2],
                inner: vec![2, 1],
                expect: vec![vec![1, 1], vec![2, 2], vec![1, 1], vec![2, 2]],
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
                    rows.push(make_i64s_row([*item]));
                }
                outer_batches.push(Batch::new(rows));
            }
            let mut inner_batches = Vec::new();
            for chunk in &item.inner.iter().chunks(batch_size) {
                let mut rows = Vec::new();
                for item in chunk {
                    rows.push(make_i64s_row([*item]));
                }
                inner_batches.push(Batch::new(rows));
            }
            let mut alloc = RefCell::new(MemoryAllocator::new());

            /* let in_queue = Inmem::new();
            let out_queue = Inmem::new(); */
            /* in_queue.enqueue(1, inner_rows);
            out_queue.enqueue(1, outer_rows); */
            let config = Config {
                bucket_size: 2,
                max_size_per_partition: 2,
                batch_size: 2,
                left_key_offset,
                right_key_offset,
            };
            let mut joiner = GraceHashJoiner::new(
                config,
                outer_batches.into_iter(),
                inner_batches.into_iter(),
                || -> Rc<dyn PartitionedQueue> { alloc.borrow_mut().alloc() },
                /* Rc::new(out_queue),
                Rc::new(in_queue), */
            );
            let mut ret: Vec<Vec<i64>> = Vec::new();
            // joined result should be 1,1|1,1|1,1|1,1
            while let Some(b) = joiner.next() {
                for row in b.data() {
                    let joined_key = FromBytes::read_from(&row.inner[..8]).unwrap();
                    let other_data = FromBytes::read_from(&row.inner[8..]).unwrap();
                    ret.push(vec![joined_key, other_data]);
                }
            }
            // let expect = [[1, 1], [1, 1], [1, 1], [1, 1]];

            println!("{:?}", ret);
            assert_eq!(
                item.expect.len(),
                ret.len(),
                "wrong number of rows returned"
            );
            assert!(item
                .expect
                .iter()
                .zip(ret.iter())
                .all(|(joined_row, expect_row)| is_all_the_same(
                    joined_row.iter(),
                    expect_row.iter()
                )));
        }
    }

    #[test]
    fn test_inmem_h_joiner() {
        struct TestCase {
            outer: Vec<i64>,
            inner: Vec<i64>,
            expect: Vec<Vec<i64>>,
        }
        let tcases = vec![
            TestCase {
                outer: vec![1, 1, 1, 1],
                inner: vec![1, 2, 2, 2],
                expect: vec![vec![1, 1], vec![1, 1], vec![1, 1], vec![1, 1]],
            },
            TestCase {
                outer: vec![1, 2, 1, 2],
                inner: vec![2, 1],
                expect: vec![vec![1, 1], vec![2, 2], vec![1, 1], vec![2, 2]],
            },
        ];
        for item in &tcases {
            let p_index = 1;
            let batch_size = 2;
            let left_key_offset = 8; // first 8 bytes represents join key
            let right_key_offset = 8;

            let mut outer_rows = Vec::new();
            for i in &item.outer {
                outer_rows.push(make_i64s_row([*i]));
            }
            let mut inner_rows = Vec::new();
            for i in &item.inner {
                inner_rows.push(make_i64s_row([*i]));
            }

            let in_queue = Inmem::new();
            let out_queue = Inmem::new();
            in_queue.enqueue(1, inner_rows);
            out_queue.enqueue(1, outer_rows);
            let mut joiner = HashJoiner::new(
                Rc::new(out_queue),
                Rc::new(in_queue),
                p_index,
                batch_size,
                left_key_offset,
                right_key_offset,
            );
            let mut ret: Vec<Vec<i64>> = Vec::new();
            // joined result should be 1,1|1,1|1,1|1,1
            while let Some(b) = joiner.next() {
                for row in b.data() {
                    let joined_key = FromBytes::read_from(&row.inner[..8]).unwrap();
                    let other_data = FromBytes::read_from(&row.inner[8..]).unwrap();
                    ret.push(vec![joined_key, other_data]);
                }
            }
            // let expect = [[1, 1], [1, 1], [1, 1], [1, 1]];

            // println!("{:?}", ret);
            assert_eq!(
                item.expect.len(),
                ret.len(),
                "wrong number of rows returned"
            );
            assert!(item
                .expect
                .iter()
                .zip(ret.iter())
                .all(|(joined_row, expect_row)| is_all_the_same(
                    joined_row.iter(),
                    expect_row.iter()
                )));
        }
    }

    fn is_all_the_same<T>(left: impl Iterator<Item = T>, right: impl Iterator<Item = T>) -> bool
    where
        T: Eq,
    {
        left.zip(right).all(|(a, b)| a == b)
    }
}
