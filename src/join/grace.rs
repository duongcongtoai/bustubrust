#[allow(dead_code)]
use core::fmt::Formatter;
use std::collections::HashMap;
use std::fmt::Debug;
use std::rc::Rc;
use twox_hash::xxh3::hash64_with_seed;

struct HashJoiner {
    outer_queue: Rc<dyn PartitionedQueue>,
    inner_queue: Rc<dyn PartitionedQueue>,
    p_index: usize,
    batch_size: usize,
    built: bool,
    htable: HashMap<Vec<u8>, Vec<Row>>,
    left_key_offset: usize,
    right_key_offset: usize,
    // unfinished_batch: Option<Vec<Row>>,
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

// TODO: add fallback to merge join, if partition contain duplicate joined rows count
// that takes more than inmem partition
struct GraceHashJoiner<F>
where
    F: Fn() -> Rc<dyn PartitionedQueue>,
{
    config: Config,
    stack: Vec<PartitionLevel>,
    undone_joining_partition: Option<HashJoiner>,
    queue_allocator: F,
}
struct PartitionLevel {
    level: usize,
    map: HashMap<usize, PInfo>,
    fallback_partitions: Vec<usize>,
    outer_queue: Rc<dyn PartitionedQueue>,
    inner_queue: Rc<dyn PartitionedQueue>,
}

pub trait PartitionedQueue {
    fn enqueue(&self, partition_idx: usize, data: Vec<Row>);
    fn dequeue(&self, partition_idx: usize, size: usize) -> Option<Batch>;
    fn id(&self) -> usize;
}

struct PInfo {
    parent_size: usize,
    memsize: usize,
}
pub struct Batch {
    inner: Vec<Row>,
}
impl Debug for Batch {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let st = self
            .inner
            .iter()
            .map(|row| row.string_data(8))
            .collect::<Vec<String>>()
            .join(",");

        f.write_str("{")?;
        f.write_str(&st)?;
        f.write_str("}")?;
        Ok(())
    }
}
impl Debug for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(&self.string_data(8))
    }
}
impl Batch {
    pub fn new(r: Vec<Row>) -> Self {
        Batch { inner: r }
    }
    pub fn data(&self) -> &Vec<Row> {
        &self.inner
    }
}

#[derive(Clone)]
pub struct Row {
    pub inner: Vec<u8>,
}
impl Row {
    fn new(inner: Vec<u8>) -> Self {
        Row { inner }
    }
    pub fn string_data(&self, key_offset: usize) -> String {
        String::from_utf8(self.inner[key_offset..].to_vec()).unwrap()
    }
}

impl HashJoiner {
    /* fn left_joined_key<'a>(self, r: &'a Row) -> &'a [u8] {
        &r.inner[..self.left_key_offset]
    } */
    fn joined_key<'a>(key_offset: usize, r: &'a Row) -> &'a [u8] {
        &r.inner[..key_offset]
    }
    fn joined_data<'a>(key_offset: usize, r: &'a Row) -> &'a [u8] {
        &r.inner[key_offset..]
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
            for (idx, item) in outer_batch.inner.iter().enumerate() {
                let joined_key = Self::joined_key(self.left_key_offset, &item);
                match self
                    .htable
                    .get(Self::joined_key(self.left_key_offset, &item))
                {
                    None => {
                        continue;
                    }
                    Some(inner_matches) => {
                        for inner_match in inner_matches {
                            let mut joined_row = item.clone();
                            let inner_data = Self::joined_data(self.right_key_offset, &inner_match);
                            joined_row.inner.extend(inner_data);
                            ret.push(joined_row);
                        }
                        // TODO need to check batch_size
                        // return all here for now
                        // if ret.len() == self.batch_size {
                        // self.unfinished_batch = Some(ret);
                        // return Some(Batch::new(ret));
                        // }
                    }
                }
            }
        }
        if ret.len() > 0 {
            return Some(Batch::new(ret));
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

impl<F> GraceHashJoiner<F>
where
    F: Fn() -> Rc<dyn PartitionedQueue>,
{
    fn partition_batch(b: Batch, partition_infos: &mut PartitionLevel, c: Config, is_inner: bool) {
        let mut hash_result: Vec<Vec<Row>> = Vec::new();
        for _ in 0..c.bucket_size {
            hash_result.push(Vec::new())
        }
        let queuer = match is_inner {
            true => &partition_infos.inner_queue,
            false => &partition_infos.outer_queue,
        };

        let key_offset = match is_inner {
            true => c.right_key_offset,
            false => c.left_key_offset,
        };

        let this_level = partition_infos.level;

        for item in b.inner {
            let h = Self::hash(
                Self::get_key(key_offset, &item),
                c.bucket_size as u64,
                this_level as u64,
            );
            hash_result[h].push(item);
        }

        for (partition_idx, same_buckets) in hash_result.into_iter().enumerate() {
            let bucket_length = same_buckets.len();

            queuer.enqueue(partition_idx, same_buckets);

            // only care about inner input
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
    fn _find_next_fallback_partition(
        &mut self,
    ) -> Option<(usize, Rc<dyn PartitionedQueue>, Rc<dyn PartitionedQueue>)> {
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
            while let Some((p_index, _, outer_queue, inner_queue)) =
                self._find_next_inmem_sized_partition(self.config.max_size_per_partition)
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
            while let Some((p_index, outer_queue, inner_queue)) =
                self._find_next_fallback_partition()
            {
                // p_index is a partition that even if partition one more time, its may not fit in
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
            while let Some(batch) = current_partition
                .outer_queue
                .dequeue(*parent_p_index, config.batch_size)
            {
                Self::partition_batch(batch, &mut new_level, config, false);
            }
            while let Some(batch) = current_partition
                .inner_queue
                .dequeue(*parent_p_index, config.batch_size)
            {
                Self::partition_batch(batch, &mut new_level, config, true);
            }
            let fallbacks = vec![];
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
        ret
    }
    // we track if there exists a bucket with length > max-size per partition
    fn new(
        c: Config,
        mut left: impl Iterator<Item = Batch>,
        mut right: impl Iterator<Item = Batch>,
        queue_allocator: F,
    ) -> Self {
        let outer_queue = queue_allocator();
        let inner_queue = queue_allocator();
        let mut joiner = GraceHashJoiner {
            config: c,
            stack: Vec::new(),
            undone_joining_partition: None,
            queue_allocator,
        };

        let map = HashMap::new();
        let mut first_level_partitions = PartitionLevel {
            map,
            outer_queue,
            inner_queue,
            level: 0,
        };
        'until_drain_all: loop {
            match left.next() {
                Some(left_batch) => {
                    Self::partition_batch(
                        // &outer_queue,
                        left_batch,
                        &mut first_level_partitions,
                        joiner.config,
                        false,
                    );
                    match right.next() {
                        Some(right_batch) => {
                            Self::partition_batch(
                                // &inner_queue,
                                right_batch,
                                &mut first_level_partitions,
                                joiner.config,
                                true,
                            );
                        }
                        None => {}
                    };
                }
                None => {
                    match right.next() {
                        Some(right_batch) => {
                            Self::partition_batch(
                                // &inner_queue,
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

        joiner.stack.push(first_level_partitions);
        return joiner;
    }

    fn hash(bytes: &[u8], bucket_size: u64, seed: u64) -> usize {
        let ret = hash64_with_seed(bytes, seed);
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
    use std::cmp::Ordering::{self, Equal};
    use std::rc::Rc;
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
                left_key_offset,
                right_key_offset,
            };
            let mut joiner = GraceHashJoiner::new(
                config,
                outer_batches.into_iter(),
                inner_batches.into_iter(),
                || -> Rc<dyn PartitionedQueue> { alloc.borrow_mut().alloc() },
            );
            let mut ret: Vec<(i64, Vec<u8>)> = Vec::new();
            // joined result should be 1,1|1,1|1,1|1,1
            while let Some(b) = joiner.next() {
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
                Rc::new(out_queue),
                Rc::new(in_queue),
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
}
