use crate::{
    bpm::BufferPoolManager,
    sql::{
        insert::{Insert, InsertPlan},
        join::{
            grace::{GraceHashJoinPlan, GraceHashJoiner, PartitionedQueue},
            queue::MemoryAllocator,
        },
        scan::{SeqScanPlan, SeqScanner},
        tx::Txn,
        util::RawInput,
        Batch, Column, Error, PartialResult, Row, Schema, SqlResult, TableMeta,
    },
};
use core::cell::RefCell;
use serde_derive::{Deserialize, Serialize};
use std::rc::Rc;

#[derive(Clone)]
pub struct ExecutionContext {
    storage: Rc<dyn Storage>,
    // bpm: Rc<BufferPoolManager>,
    pub txn: Txn,
    queue: Rc<RefCell<MemoryAllocator>>, // TODO: make this into a trait object
}

impl ExecutionContext {
    pub fn new(store: Rc<dyn Storage>) -> Self {
        let inmem = MemoryAllocator::new();
        ExecutionContext {
            txn: Txn {},
            storage: store,
            queue: Rc::new(RefCell::new(inmem)),
        }
    }
    pub fn get_txn(&self) -> &Txn {
        &self.txn
    }

    /* pub fn get_bpm(&self) -> Rc<BufferPoolManager> {
        self.bpm.clone()
    } */

    pub fn get_storage(&self) -> Rc<dyn Storage> {
        self.storage.clone()
    }

    // F: Fn() -> Rc<dyn PartitionedQueue>,

    pub fn new_queue(&self) -> Rc<dyn PartitionedQueue> {
        (*self.queue).borrow_mut().alloc()
    }
}

pub trait Operator {
    fn next(&mut self) -> SqlResult<PartialResult>;
}

pub enum PlanType {
    SeqScan(SeqScanPlan),
    RawInput(RawInput),
    Insert(InsertPlan),
    GraceHashJoin(GraceHashJoinPlan),
    IndexScan,
    Update,
    Delete,
    Aggregation,
    Limit,
    HashJoin,
}
/* pub enum SubPlan {
    SeqScan(SeqScanPlan),
    RawInput(RawInput),
} */

pub struct IterOp {
    inner: Box<dyn Operator>,
    err: Option<Error>,
}
/* impl IterOp {
    // operator may fail mid way, after executing, always check error
    pub fn error(&self) -> &Option<Error> {
        &self.err
    }
}
impl IntoIterator for Batch {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;

    fn into_iter(self) -> std::vec::IntoIter<Row> {
        self.inner.into_iter()
    }
}

impl Iterator for IterOp {
    type Item = Batch;
    fn next(&mut self) -> Option<Batch> {
        let next_ret = self.inner.next();
        match next_ret {
            Ok(partial_ret) => {
                if partial_ret.done {
                    return None;
                }
                return Some(partial_ret.inner);
            }
            Err(some_err) => {
                self.err = Some(some_err);
                return None;
            }
        }
    }
} */
pub struct ResultSet {
    pub rows: Vec<Row>,
}
pub struct Executor {}
impl Executor {
    // if the result set cannot be contained in memory, use some stuff like
    // iterator or async stream
    pub fn execute_lazy<O: Operator>(plan_type: PlanType, ctx: ExecutionContext) {
        todo!()
    }

    // TODO: maybe return some async iter like stream in the future
    pub fn execute(plan: PlanType, ctx: ExecutionContext) -> SqlResult<ResultSet> {
        let mut operator: Box<dyn Operator> = Self::create_operator(plan, ctx);

        let mut ret = vec![];

        // TODO: not sure if this calls next() for non_result operator
        loop {
            let partial_ret = operator.next()?;
            if partial_ret.done {
                break;
            }
            for item in partial_ret.inner.inner {
                ret.push(item);
            }
        }

        Ok(ResultSet { rows: ret })
    }

    pub fn create_operator(plan_type: PlanType, ctx: ExecutionContext) -> Box<dyn Operator> {
        match plan_type {
            PlanType::SeqScan(plan) => {
                Box::new(SeqScanner::from_plan(plan, ctx.clone())) as Box<dyn Operator>
            }
            PlanType::Insert(plan) => Box::new(Insert::new(plan, ctx.clone())),
            PlanType::RawInput(raw) => Box::new(raw),
            PlanType::GraceHashJoin(plan) => {
                let cloned = ctx.clone();
                Box::new(GraceHashJoiner::from_plan(
                    plan,
                    move || -> Rc<dyn PartitionedQueue> { cloned.new_queue() },
                    ctx.clone(),
                ))
            }
            _ => {
                todo!("todo")
            }
        }
    }
    pub fn create_from_subplan_operator(
        plan_type: PlanType,
        ctx: ExecutionContext,
    ) -> Box<dyn Operator> {
        match plan_type {
            PlanType::SeqScan(plan) => {
                Box::new(SeqScanner::from_plan(plan, ctx)) as Box<dyn Operator>
            }
            PlanType::RawInput(raw) => Box::new(raw),
            _ => {
                todo!("todo")
            }
        }
    }
}
pub trait Catalog {
    fn create_table(&self, tablename: String, schema: Schema) -> SqlResult<TableMeta>;

    fn get_table(&self, tablename: String) -> SqlResult<TableMeta>;
}

pub struct Tuple {
    pub rid: RID,
    pub data: Vec<u8>,
}
impl Tuple {
    pub fn construct(rid: RID, data: Vec<u8>) -> Self {
        Tuple { data, rid }
    }
    pub fn new(data: Vec<u8>) -> Self {
        Tuple {
            data,
            rid: RID::default(),
        }
    }
    pub fn new_multi(data: Batch) -> Vec<Self> {
        let mut ret = vec![];
        for item in data.inner {
            ret.push(Self::new(item.inner));
        }
        ret
    }
}
pub type RID = u64;
// #[derive(Default)]
/* pub struct RID {
    page_id: i32,
    slot_num: u32,
} */

pub trait Storage: Catalog {
    // TODO: batch insert
    fn insert_tuple(&self, table: &str, tuple: Tuple, txn: &Txn) -> SqlResult<RID>;
    fn insert_tuples(&self, table: &str, tuple: Vec<Tuple>, txn: &Txn) -> SqlResult<RID>;
    fn mark_delete(&self, table: &str, rid: RID, txn: &Txn) -> SqlResult<()>;
    fn apply_delete(&self, table: &str, rid: RID, txn: &Txn) -> SqlResult<()>;
    fn get_tuple(&self, table: &str, rid: RID, txn: &Txn) -> SqlResult<Tuple>;
    fn scan(&self, table: &str, txn: &Txn) -> SqlResult<Box<dyn Iterator<Item = Tuple>>>;
}
#[cfg(test)]
pub mod tests {
    use crate::{
        sql::{
            exe::{Executor, PlanType},
            scan::SeqScanPlan,
            table_gen::GenTableUtil,
            ExecutionContext,
        },
        storage::sled::Sled,
    };
    use std::{env::current_dir, module_path, rc::Rc};
    use tempfile::NamedTempFile;

    use super::Storage;
    #[test]
    fn test_seq_scan() {
        let dep = setup();
        let mut cur_dir = current_dir().expect("failed getting current dir");
        /* cur_dir.push("test_data");
        cur_dir.push("test_1"); */
        let file = format!("{}/test_data/test_1.json", cur_dir.to_str().unwrap());
        println!("file {}", file);
        dep.gen_table.gen(file).expect("failed generating table");
        let executor = dep.executor;
        let seq_scan_plan = SeqScanPlan {
            table: "test_1".to_string(),
        };
        let ctx = ExecutionContext::new(dep.storage.clone());
        let ret = Executor::execute(PlanType::SeqScan(seq_scan_plan), ctx)
            .expect("failed executing seq scan");
        for item in ret.rows {}
    }

    pub struct Dependencies {
        storage: Rc<dyn Storage>,
        executor: Executor,
        gen_table: GenTableUtil,
    }

    pub fn setup() -> Dependencies {
        let file = NamedTempFile::new().expect("failed creating temp file");
        let path = file.into_temp_path().to_str().unwrap().to_string();
        let db = Sled::new(path).expect("failed creating sled");
        let cloned = Rc::new(db);
        Dependencies {
            storage: cloned.clone(),
            executor: Executor {},
            gen_table: GenTableUtil {
                ctx: ExecutionContext::new(cloned),
            },
        }
    }
}
