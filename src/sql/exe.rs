use super::join::queue::MemoryAllocator;
use super::util::RawInput;
use super::{Batch, SqlResult};
use crate::bpm::BufferPoolManager;
use crate::sql::join::grace::GraceHashJoinPlan;
use crate::sql::join::grace::GraceHashJoiner;
use crate::sql::join::grace::PartitionedQueue;
use crate::sql::scan::SeqScanPlan;
use crate::sql::scan::SeqScanner;
use crate::sql::tx::Txn;
use crate::sql::Error;
use crate::sql::PartialResult;
use crate::sql::Row;
use core::cell::RefCell;
use serde_derive::{Deserialize, Serialize};
use std::rc::Rc;

#[derive(Clone)]
pub struct ExecutionContext {
    storage: Rc<dyn Storage>,
    bpm: Rc<BufferPoolManager>,
    txn: Txn,
    queue: Rc<RefCell<MemoryAllocator>>, // TODO: make this into a trait object
}

impl ExecutionContext {
    pub fn get_txn(&self) -> &Txn {
        &self.txn
    }

    pub fn get_bpm(&self) -> Rc<BufferPoolManager> {
        self.bpm.clone()
    }

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
    IndexScan,
    Insert,
    Update,
    Delete,
    Aggregation,
    Limit,
    HashJoin,
    GraceHashJoin(GraceHashJoinPlan),
}
pub enum SubPlan {
    SeqScan(SeqScanPlan),
    RawInput(RawInput),
}

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
    rows: Vec<Row>,
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
        plan_type: SubPlan,
        ctx: ExecutionContext,
    ) -> Box<dyn Operator> {
        match plan_type {
            SubPlan::SeqScan(plan) => {
                Box::new(SeqScanner::from_plan(plan, ctx)) as Box<dyn Operator>
            }
            SubPlan::RawInput(raw) => Box::new(raw),
            _ => {
                todo!("todo")
            }
        }
    }
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct TableMeta {
    pub schema: Schema,
    pub name: String,
    pub oid: u32,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct Schema {
    pub columns: Vec<Column>,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct Column {
    name: String,
    fixed_length: usize,
    variable_length: usize,
    type_id: DataType,
}
impl Column {
    pub fn new(name: String, type_id: DataType) -> Self {
        let fixed_length: usize;
        match type_id {
            DataType::BOOL | DataType::TINYINT => fixed_length = 1,
            DataType::INTEGER => fixed_length = 4,
            _ => panic!("unimplmeneted"),
        }
        Column {
            name,
            fixed_length,
            type_id,
            variable_length: 0,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum DataType {
    INVALID,
    BOOL,
    TINYINT,
    SMALLINT,
    INTEGER,
    BIGINT,
    DECIMAL,
    VARCHAR,
    TIMESTAMP,
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
}
pub type RID = u64;
// #[derive(Default)]
/* pub struct RID {
    page_id: i32,
    slot_num: u32,
} */

pub trait Storage: Catalog {
    fn insert_tuple(&self, table: &str, tuple: Tuple, txn: &Txn) -> SqlResult<RID>;
    fn mark_delete(&self, table: &str, rid: RID, txn: &Txn) -> SqlResult<()>;
    fn apply_delete(&self, table: &str, rid: RID, txn: &Txn) -> SqlResult<()>;
    fn get_tuple(&self, table: &str, rid: RID, txn: &Txn) -> SqlResult<Tuple>;
    fn scan(&self, table: &str, txn: &Txn) -> SqlResult<Box<dyn Iterator<Item = Tuple>>>;
}
