use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem;
use std::rc::Rc;

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

struct GraceHashJoiner {
    bucket_size: usize,
    max_size_per_partition: usize,
    partition_queue_inner: Box<dyn PartitionedQueue>,
    partition_queue_outer: Box<dyn PartitionedQueue>,
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

trait PartitionedQueue {
    fn enqueue(&self, partition_idx: usize, data: Vec<Row>);
    fn dequeue(&self, partition_idx: usize, size: usize) -> Option<Batch>;
}

// struct RcDynPartitionQueue(Rc<dyn PartitionedQueue>);

struct PInfo {
    parent_size: usize,
    memsize: usize,
}
struct Batch {
    inner: Vec<Row>,
}

#[derive(Clone)]
struct Row {
    inner: Vec<u8>,
}

/* impl Row {
    fn key(&self) -> Vec<u8> {

    }
}  */

impl HashJoiner {
    fn left_joined_key<'a>(self, r: &'a Row) -> &'a [u8] {
        &r.inner[..self.left_key_offset]
    }
    fn right_joined_key<'a>(self, r: &'a Row) -> &'a [u8] {
        &r.inner[..self.right_key_offset]
    }

    fn _build(&mut self) {
        let htable = self.htable;
        while let Some(batch) = self.inner_queue.dequeue(self.p_index, self.batch_size) {
            for item in batch.inner {
                let key = self.right_joined_key(&item);
                match htable.get_mut(key) {
                    Some(mut datas) => {
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
        let ret = Vec::new();
        while let Some(outer_batch) = self.outer_queue.dequeue(self.p_index, self.batch_size) {
            for item in outer_batch.inner {
                match self.htable.get(self.left_joined_key(&item)) {
                    None => continue,
                    Some(inner_matches) => {
                        for inner_match in inner_matches {
                            let mut joined_row = item.clone();
                            joined_row.inner.extend(inner_match.inner);
                            ret.push(joined_row);
                        }
                    }
                }
            }
        }
        return Some(Batch { inner: ret });
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
        &mut self,
        input_idx: usize,
        b: Batch,
        partition_infos: &mut HashMap<usize, PInfo>,
    ) {
        let queuer = &self.partition_queue_outer;
        if input_idx == 1 {
            queuer = &self.partition_queue_inner;
        }
        let mut hash_result: Vec<Vec<Row>> = Vec::new();
        for _ in 0..self.bucket_size {
            hash_result.push(Vec::new())
        }
        for item in b.inner {
            let h = Self::hash(self.get_key(input_idx, &item), self.bucket_size as u64);
            hash_result[h].push(item);
        }
        for (partition_idx, same_buckets) in hash_result.into_iter().enumerate() {
            let bucket_length = same_buckets.len();
            // let partition_idx = self.current_partition_offset + bucket_idx;
            queuer.enqueue(partition_idx, same_buckets);

            // only care about inner input
            if input_idx == 1 {
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
        partition_infos: &mut HashMap<usize, PInfo>,
    ) -> Option<(usize, PInfo)> {
        let mut found_index = None;
        for (index, item) in partition_infos {
            if item.memsize <= self.max_size_per_partition {
                found_index = Some(index);
                break;
            }
        }
        match found_index {
            None => None,
            Some(index) => Some((*index, partition_infos.remove(&index).unwrap())),
        }
    }

    fn _new_hash_joiner(&mut self, p_index: usize) -> HashJoiner {
        let p = &self
            .partition_queue_inner
            .dequeue(p_index, self.batch_size)
            .unwrap();

        let st = HashJoiner {
            batch_size: self.batch_size,
            htable: HashMap::new(),
            outer_queue: Rc::from(self.partition_queue_outer),
            inner_queue: Rc::from(self.partition_queue_inner),
            built: false,
            p_index,
            left_key_offset: 1,
            right_key_offset: 1,
        };
        return st;
    }

    fn next(&mut self) -> Option<Batch> {
        if let Some(inmem_joiner) = self.undone_joining_partition {
            match inmem_joiner.next() {
                None => {
                    self.undone_joining_partition = None;
                }
                Some(batch_result) => {
                    return Some(batch_result);
                }
            };
        };

        let mut stack = self.stack;
        while stack.len() > 0 {
            let mut cur_partitions = stack.last().unwrap();
            while let Some((p_index, inmem_p_info)) =
                self._find_next_inmem_sized_partition(&mut cur_partitions)
            {
                let inmem_joiner = self._new_hash_joiner(p_index);
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

            // recursive partition
            if cur_partitions.len() > 0 {
                for (parent_p_index, pinfo) in cur_partitions {
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
                }
            }
            stack.pop();
        }
        None

        // Find next partition that we can process without having to
        // recursively repartition.
    }
    // we track if there exists a bucket with length > max-size per partition
    fn new(
        c: Config,
        left: impl Iterator<Item = Batch>,
        right: impl Iterator<Item = Batch>,
        outer_queue: Box<dyn PartitionedQueue>,
        inner_queue: Box<dyn PartitionedQueue>,
    ) -> Self {
        let mut hash_result: Vec<Vec<Row>> = Vec::new();
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
                    joiner.partition_batch(0, left_batch, &mut first_level_partitions);
                    match right.next() {
                        Some(right_batch) => {
                            joiner.partition_batch(1, right_batch, &mut first_level_partitions);
                            //left_batch,right_batch
                        }
                        None => {}
                    };
                }
                None => {
                    match right.next() {
                        Some(right_batch) => {
                            joiner.partition_batch(1, right_batch, &mut first_level_partitions);
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
        let ret = xxhash_rust::xxh3::xxh3_64(bytes);
        return ret % bucket_size;
    }
    fn get_key<'a>(&self, input_idx: usize, r: &'a Row) -> &'a [u8] {
        if input_idx == 1 {
            &r.inner[..self.left_key_offset]
        } else {
            &r.inner[..self.right_key_offset]
        }
    }
}
