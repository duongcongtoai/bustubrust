use crate::bpm::{BufferPoolManager, Frame, Replacer};
use crate::error::{Error, Result};
use crate::replacer::LRURepl;
use crate::sql::executor::Executor;
use crate::sql::tx::Transaction;
use crate::sql::Value;
use crate::sql::{ColumnedBatch, ResultSet};
use iota::iota;
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;

// TODO: BufferPoolManager should have generic over LRURepl, but this module should not not about
// that type parameter, maybe using associated types????
pub struct GraceHashJoin<T: Transaction> {
    bpm: RefCell<BufferPoolManager<LRURepl>>,
    left: Box<dyn Executor<T>>,
    left_field: usize,
    right: Box<dyn Executor<T>>,
    right_field: usize,
    state: PartitionState,
    // TODO: for each input, implement an in-memory input container that makes call to
    // bpm for enqueuing/dequeing the batch data on demand
    //
}
type PartitionState = i64;
iota! {
    const INITIAL_PARTITIONING: PartitionState = 0 << iota;
        ,PROCESS_NEW_PARTITION
        ,PROCESSING
        ,FINISHED
}

impl<T: Transaction, F: Fn(Frame, Frame) -> Box<dyn Executor<T>>> Executor<T>
    for GraceHashJoin<T, F>
{
    /// When the tables do not fit on main memory, the DBMS has to swap tables in and out essentially at random,
    /// which leads to poor performance. The Grace Hash Join is an extension of the basic hash join that also hashes
    /// the inner table into partitions that are written out to disk.
    ///
    /// • Phase #1 – Build: First, scan both the outer and inner tables and populate a hash table using the hash
    /// function h1 on the join attributes. The hash table’s buckets are written out to disk as needed. If a single
    /// bucket does not fit in memory, the DBMS can use recursive partitioning with different hash function
    /// h2 (where h1 6= h2) to further divide the bucket. This can continue recursively until the buckets fit
    /// into memory.
    ///
    /// • Phase #2 – Probe: For each bucket level, retrieve the corresponding pages for both outer and inner
    /// tables. Then, perform a nested loop join on on the tuples in those two pages. The pages will fit in
    /// memory, so this join operation will be fast.
    /// Referenec: https://15445.courses.cs.cmu.edu/fall2021/notes/10-joins.pdf
    /// CockcroachDB: https://github.com/cockroachdb/cockroach/issues/43790
    fn next(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        loop {
            match self.state {
                INITIAL_PARTITIONING => self.initial_partition(txn),
                PROCESS_NEW_PARTITION => self.process_new_partition(txn),
                PROCESSING => self.processing(txn),
                FINISHED => {
                    self.finish(txn);
                    break;
                }
            };
        }
        /* let bpm = self.bpm.borrow();
        self.left.next(txn);
        bpm.new_page(); */
        Err(Error::Abort)
    }
}
struct FramedScanner {
    f: Frame,
}
impl FramedScanner {
    fn new(f: Frame) -> Self {
        FramedScanner { f }
    }
}

impl<T: Transaction> Executor<T> for FramedScanner {
    fn next(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {}
}

impl<T: 'static + Transaction> GraceHashJoin<T> {
    fn new(
        bpm: BufferPoolManager<LRURepl>,
        left: Box<dyn Executor<T>>, // original left source
        left_field: usize,
        right: Box<dyn Executor<T>>, // original right source
        right_field: usize,
    ) -> Self {
        GraceHashJoin {
            bpm: RefCell::new(bpm),
            left,
            right,
            left_field,
            right_field,
            state: INITIAL_PARTITIONING,
        }
    }
    fn in_mem_op_constructor(f1: Frame, f2: Frame) -> Box<dyn Executor<T>> {
        HashInnerJoin::new(
            Box::new(FramedScanner::new(f1)),
            0,
            Box::new(FramedScanner::new(f2)),
            0,
            false,
        )
    }

    fn initial_partition(self: Box<Self>, txn: &mut T) {
        let left_batch = self.left.next(txn);
        let right_batch = self.right.next(txn);
    }
    fn process_new_partition(self: Box<Self>, txn: &mut T) {}
    fn processing(self: Box<Self>, txn: &mut T) {}
    fn finish(self: Box<Self>, txn: &mut T) {}

    fn partition_batch(self: Box<Self>, ret: ColumnedBatch, idx: usize, parent_mem_size: i64) {
        if ret.len() == 0 {
            return;
        }
    }

    // result stores info about at which partition, which tuples in the input batch belongs to
    fn _distribute_batch(self: Box<Self>, b: ColumnedBatch) -> Vec<Vec<i64>> {
        let total_tuple = b.len();
    }

    fn xxhash3(val: &[u8]) -> u64 {
        xxhash_rust::xxh3::xxh3_64(val)
    }
}

pub struct HashInnerJoin<T: Transaction> {
    left: Box<dyn Executor<T>>,
    left_field: usize, // index of the joined field inside the tuple of left table
    right: Box<dyn Executor<T>>,
    right_field: usize, // index of the joined field inside the tuple of the right table
    outer: bool,
}
impl<T: Transaction> Executor<T> for HashInnerJoin<T> {
    fn next(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        if let ResultSet::Query {
            rows: outer_rows,
            columns: mut outer_columns,
        } = self.left.next(txn)?
        {
            let (l, r) = (self.left_field, self.right_field);
            if let ResultSet::Query {
                rows: right_rows,
                columns: right_columns,
            } = self.right.next(txn)?
            {
                let inner_table: HashMap<Value, Vec<Value>> = right_rows
                    .map(|r_tuple| match r_tuple {
                        Ok(values) => {
                            if values.len() <= r {
                                return Err(Error::Internal(
                                    "right index out of bound".to_string(),
                                ));
                            }
                            let joined_key = values[r].clone();
                            return Ok((joined_key, values));
                        }
                        Err(err) => Err(err),
                    })
                    .collect::<Result<_>>()?;
                outer_columns.extend(right_columns);
                // iterate outer rows, probe inner table, if match omit, else do nothing
                // this is like an inner join
                let joined_rows = outer_rows.filter_map(move |tuple_result| match tuple_result {
                    Ok(mut tuple) => {
                        if tuple.len() <= l {
                            return Some(Err(Error::Value("left index out of bound".to_string())));
                        }

                        let joined_field = tuple[l].clone();
                        match inner_table.get(&joined_field) {
                            Some(hit) => {
                                //TODO: consider late vs early materialization
                                tuple.extend(hit.clone());
                                return Some(Ok(tuple));
                            }
                            None => return None,
                        };
                    }
                    Err(err) => Some(Err(err)),
                });
                return Ok(ResultSet::Query {
                    columns: outer_columns,
                    rows: Box::new(joined_rows),
                });
            }
        };
        Err(Error::Internal("unimplemented".into()))
    }
}

impl<T: Transaction> HashInnerJoin<T> {
    pub fn new(
        left: Box<dyn Executor<T>>,
        left_field: usize,
        right: Box<dyn Executor<T>>,
        right_field: usize,
        outer: bool,
    ) -> Box<Self> {
        if outer {
            panic!("unimplemented");
        }
        Box::new(HashInnerJoin {
            left,
            left_field,
            right,
            right_field,
            outer,
        })
    }
}
