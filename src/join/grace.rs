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

struct GraceHashJoiner {
    bucket_size: usize,
    max_size_per_partition: usize,
    partition_queue_inner: Rc<dyn PartitionedQueue>,
    partition_queue_outer: Rc<dyn PartitionedQueue>,
    batch_size: usize,
    stack: Vec<HashMap<usize, PInfo>>,
    undone_joining_partition: Option<HashJoiner>,
    // current_partition_offset: usize,
    // NOTE: we only store partition info of inner
    // partition_infos: HashMap<usize, PInfo>
    left_key_offset: usize,
    right_key_offset: usize,
}

// it should hold internal pools of allocatable disk
// it memorize which partition points to which file
// each file operation only needs enqueing (append to file), and dequeuing reading from current
// offset
// struct PartitionedQueue {}

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
impl GraceHashJoiner {
    fn partition_batch(
        queuer: &Rc<dyn PartitionedQueue>,
        bucket_size: usize,
        b: Batch,
        partition_infos: &mut HashMap<usize, PInfo>,
        key_offset: usize,
        is_inner: bool,
        // right_key_offset: usize,
    ) {
        /* let mut queuer = &self.partition_queue_outer;
        if input_idx == 1 {
            queuer = &self.partition_queue_inner;
        } */
        let mut hash_result: Vec<Vec<Row>> = Vec::new();
        for _ in 0..bucket_size {
            hash_result.push(Vec::new())
        }
        for item in b.inner {
            let h = Self::hash(Self::get_key(key_offset, &item), bucket_size as u64);
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
    ) -> Option<(usize, PInfo)> {
        let mut found_index = None;
        let partition_infos = self.stack.last_mut().unwrap();
        for (index, item) in partition_infos.iter_mut() {
            if item.memsize <= max_size_per_partition {
                found_index = Some(*index);
                break;
            }
        }
        match found_index {
            None => None,
            Some(index) => Some((index, partition_infos.remove(&index).unwrap())),
        }
    }

    fn _new_hash_joiner(
        &self,
        p_index: usize,
        left_key_offset: usize,
        right_key_offset: usize,
    ) -> HashJoiner {
        /* let p = self
        .partition_queue_inner
        .dequeue(p_index, self.batch_size)
        .unwrap(); */

        let st = HashJoiner {
            batch_size: self.batch_size,
            htable: HashMap::new(),
            outer_queue: self.partition_queue_outer.clone(),
            inner_queue: self.partition_queue_inner.clone(),
            built: false,
            p_index,
            left_key_offset,
            right_key_offset,
        };
        return st;
    }

    fn stack_len(&self) -> usize {
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
    }

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

        'recursiveloop: while self.stack_len() > 0 {
            while let Some((p_index, _)) = self._find_next_inmem_sized_partition(
                self.max_size_per_partition,
                // &mut cur_partitions,
            ) {
                let mut inmem_joiner =
                    self._new_hash_joiner(p_index, self.left_key_offset, self.right_key_offset);
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
            if cur_partitions.len() > 0 {
                // let st = cur_partitions.last_mut();
                if let Some(next_recursive_p) = Self::recursive_partition(
                    &self.partition_queue_outer,
                    &self.partition_queue_inner,
                    self.batch_size,
                    self.bucket_size,
                    cur_partitions,
                ) {
                    self.stack.push(next_recursive_p);
                    continue 'recursiveloop;
                }
                /* for (parent_p_index, pinfo) in cur_partitions {
                    let mut child_partitions = HashMap::new();
                    let p_queue_outer = self.partition_queue_outer;
                    let p_queue_inner = self.partition_queue_inner;
                    for (idx, queuer) in [p_queue_inner, p_queue_outer].into_iter().enumerate() {
                        while let Some(batch) = queuer.dequeue(*parent_p_index, self.batch_size) {
                            // this will create partition
                            self.partition_batch(idx, batch, &mut child_partitions);
                        }
                    }
                    cur_partitions.remove(parent_p_index);
                    stack.push(child_partitions);
                    continue;
                } */
            }
            self.stack_pop();
        }
        None

        // Find next partition that we can process without having to
        // recursively repartition.
    }
    fn recursive_partition(
        p_queue_outer: &Rc<dyn PartitionedQueue>,
        p_queue_inner: &Rc<dyn PartitionedQueue>,
        batch_size: usize,
        bucket_size: usize,
        current_partition: &mut HashMap<usize, PInfo>,
    ) -> Option<HashMap<usize, PInfo>> {
        // let cur_partitions = self.stack.last_mut().unwrap();
        let mut ret = None;
        let mut item_remove = -1;
        for (parent_p_index, _) in current_partition.iter_mut() {
            let mut child_partitions = HashMap::new();
            /* let p_queue_outer = partition_queue_outer;
            let p_queue_inner = partition_queue_inner; */
            while let Some(batch) = p_queue_outer.dequeue(*parent_p_index, batch_size) {
                // this will create partition
                Self::partition_batch(
                    &p_queue_outer,
                    bucket_size,
                    batch,
                    &mut child_partitions,
                    1,
                    false,
                );
            }
            while let Some(batch) = p_queue_inner.dequeue(*parent_p_index, batch_size) {
                // this will create partition
                Self::partition_batch(
                    &p_queue_inner,
                    bucket_size,
                    batch,
                    &mut child_partitions,
                    1,
                    false,
                );
            }

            ret = Some(child_partitions);
            item_remove = *parent_p_index as i64;
        }
        if item_remove != -1 {
            current_partition.remove(&(item_remove as usize));
        }
        ret
    }
    // we track if there exists a bucket with length > max-size per partition
    fn new(
        c: Config,
        mut left: impl Iterator<Item = Batch>,
        mut right: impl Iterator<Item = Batch>,
        outer_queue: Rc<dyn PartitionedQueue>,
        inner_queue: Rc<dyn PartitionedQueue>,
    ) -> Self {
        // let hash_result: Vec<Vec<Row>> = Vec::new();
        let mut joiner = GraceHashJoiner {
            bucket_size: c.bucket_size,
            max_size_per_partition: c.max_size_per_partition,
            left_key_offset: c.left_key_offset,
            right_key_offset: c.right_key_offset,
            batch_size: c.batch_size,
            partition_queue_outer: outer_queue,
            partition_queue_inner: inner_queue,
            stack: Vec::new(),
            undone_joining_partition: None,
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
                        &joiner.partition_queue_outer,
                        joiner.bucket_size,
                        left_batch,
                        &mut first_level_partitions,
                        joiner.left_key_offset,
                        false,
                    );
                    match right.next() {
                        Some(right_batch) => {
                            Self::partition_batch(
                                &joiner.partition_queue_inner,
                                joiner.bucket_size,
                                right_batch,
                                &mut first_level_partitions,
                                joiner.right_key_offset,
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
                                &joiner.partition_queue_inner,
                                joiner.bucket_size,
                                right_batch,
                                &mut first_level_partitions,
                                joiner.right_key_offset,
                                true,
                            );
                        }
                        None => break 'until_drain_all,
                    };
                }
            };
        }

        joiner.stack.push(first_level_partitions);
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
    use super::{HashJoiner, PartitionedQueue, Row};
    use crate::join::queue::Inmem;
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
