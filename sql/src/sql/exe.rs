use crate::{
    sql::{
        insert::InsertPlan,
        join::{
            grace::{GraceHashJoinOp, GraceHashJoinPlan, PartitionedQueue},
            queue::MemoryAllocator,
        },
        scan::SeqScanPlan,
        tx::Txn,
        util::RawInput,
        DataBlock, SqlResult,
    },
    storage::sled::Sled,
};
use datafusion::arrow::datatypes::SchemaRef;
use std::{fmt::Debug, sync::Arc};
use tempfile::NamedTempFile;

#[derive(Clone)]
pub struct ExecutionContext {
    storage: Arc<dyn Storage>,
    pub txn: Txn,
    queue: Arc<MemoryAllocator>, // TODO: make this into a trait object
}

impl ExecutionContext {
    pub fn new_for_test() -> Self {
        let inmem = MemoryAllocator::new();
        // let temp = tempfile().expect();
        let file = NamedTempFile::new().expect("failed creating temp file");
        let path = file.into_temp_path().to_str().unwrap().to_string();
        let db = Sled::new(path).expect("failed creating sled");

        ExecutionContext {
            txn: Txn::new(),
            storage: Arc::new(db),
            queue: Arc::new(inmem),
        }
    }
    pub fn new(store: Arc<dyn Storage>) -> Self {
        let inmem = MemoryAllocator::new();
        ExecutionContext {
            txn: Txn::new(),
            storage: store,
            queue: Arc::new(inmem),
        }
    }
    pub fn get_txn(&self) -> &Txn {
        &self.txn
    }

    pub fn get_storage(&self) -> Arc<dyn Storage> {
        self.storage.clone()
    }

    pub fn new_queue(&self, schema: SchemaRef) -> Arc<dyn PartitionedQueue> {
        (*self.queue).alloc(schema)
    }
}

/// Trait for types that stream [arrow::record_batch::RecordBatch]
pub trait DataIter: Iterator<Item = SqlResult<DataBlock>> {
    fn schema(self) -> SchemaRef;
}

pub type BoxedDataIter = Box<dyn DataIter>;

// pub type SendableResult = Pin<Box<dyn Future<Output = SqlResult<()>> + Send + Sync>>;
pub struct SchemaDataIter {
    inner: Box<dyn Iterator<Item = SqlResult<DataBlock>>>,
    schema: SchemaRef,
}

unsafe impl Send for SchemaDataIter {}
unsafe impl Sync for SchemaDataIter {}
impl SchemaDataIter {
    pub fn new(
        schema: SchemaRef,
        inner: Box<dyn Iterator<Item = SqlResult<DataBlock>>>,
    ) -> Box<Self> {
        Box::new(SchemaDataIter { inner, schema })
    }
}
impl DataIter for SchemaDataIter {
    fn schema(self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Iterator for SchemaDataIter {
    type Item = SqlResult<DataBlock>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait Operator: Debug + Send + Sync {
    fn execute_sync(&mut self, ctx: ExecutionContext) -> SqlResult<BoxedDataIter>;

    fn schema(&self) -> SchemaRef;
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

pub struct Executor {}
impl Executor {
    // TODO: maybe return some async iter like stream in the future
    pub fn execute(plan: PlanType, ctx: ExecutionContext) -> SqlResult<BoxedDataIter> {
        let mut operator: Box<dyn Operator> = Self::create_operator(plan, ctx.clone());
        operator.execute_sync(ctx)
    }

    pub fn create_operator(plan_type: PlanType, ctx: ExecutionContext) -> Box<dyn Operator> {
        match plan_type {
            /* PlanType::SeqScan(plan) => {
                Box::new(SeqScanner::from_plan(plan, ctx.clone())) as Box<dyn Operator>
            }
            PlanType::Insert(plan) => Box::new(Insert::new(plan, ctx.clone())),
            PlanType::RawInput(raw) => Box::new(raw), */
            PlanType::GraceHashJoin(plan) => {
                let cloned = ctx.clone();
                Box::new(GraceHashJoinOp::from_plan(plan, ctx.clone()))
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
            /* PlanType::SeqScan(plan) => {
                Box::new(SeqScanner::from_plan(plan, ctx)) as Box<dyn Operator>
            }
            PlanType::RawInput(raw) => Box::new(raw), */
            _ => {
                todo!("todo")
            }
        }
    }
}
pub trait Catalog {
    /* fn create_table(&self, tablename: String, schema: Schema) -> SqlResult<TableMeta>;

    fn get_table(&self, tablename: String) -> SqlResult<TableMeta>; */
}

pub type RID = u64;

pub trait Storage: Catalog + Sync + Send {
    // TODO: batch insert
    fn insert_tuples(
        &self,
        table: &str,
        data: BoxedDataIter,
        txn: &Txn,
    ) -> SqlResult<BoxedDataIter>;
    fn delete(&self, table: &str, data: BoxedDataIter, txn: &Txn) -> SqlResult<()>;
    fn get_tuples(&self, table: &str, rids: BoxedDataIter, txn: &Txn) -> SqlResult<BoxedDataIter>;

    fn scan(&self, table: &str, txn: &Txn) -> SqlResult<BoxedDataIter>;
}

#[cfg(test)]
pub mod tests {
    use super::Storage;
    use crate::{
        sql::{
            exe::{Executor, PlanType},
            scan::SeqScanPlan,
            table_gen::GenTableUtil,
            ExecutionContext,
        },
        storage::sled::Sled,
    };
    use std::{env::current_dir, rc::Rc, sync::Arc};
    use tempfile::NamedTempFile;

    #[test]
    fn test_seq_scan() {}

    pub struct Dependencies {
        storage: Arc<dyn Storage>,
        executor: Executor,
        gen_table: GenTableUtil,
    }

    pub fn setup() -> Dependencies {
        let file = NamedTempFile::new().expect("failed creating temp file");
        let path = file.into_temp_path().to_str().unwrap().to_string();
        let db = Sled::new(path).expect("failed creating sled");
        let cloned = Arc::new(db);
        Dependencies {
            storage: cloned.clone(),
            executor: Executor {},
            gen_table: GenTableUtil {
                ctx: ExecutionContext::new(cloned),
            },
        }
    }
}
