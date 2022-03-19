use crate::bpm::BufferPoolManager;
use crate::sql::tx::Txn;
use std::rc::Rc;

use super::SqlResult;

pub struct ExecutionContext {
    storage: Rc<dyn Storage>,
    bpm: Rc<BufferPoolManager>,
    txn: Txn,
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
}
#[derive(Clone)]
pub struct TableMeta {
    schema: Schema,
    name: String,
    oid: u32,
}
#[derive(Clone)]
pub struct Schema {
    columns: Vec<Column>,
}
#[derive(Clone)]
pub struct Column {
    name: String,
    fixed_length: usize,
    variable_length: usize,
    type_id: DataType,
}
#[derive(Clone)]
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

pub struct Tuple {}
pub struct RID {
    page_id: i32,
    slot_num: u32,
}

pub trait Storage: Catalog {
    fn insert_tuple(&self, table: &str, tuple: Tuple, rid: RID, txn: Txn) -> SqlResult<()>;
    fn mark_delete(&self, table: &str, rid: RID, txn: Txn) -> SqlResult<()>;
    fn apply_delete(&self, table: &str, rid: RID, txn: Txn) -> SqlResult<()>;
    fn get_tuple(&self, table: &str, rid: RID, txn: Txn) -> SqlResult<Tuple>;
    fn scan(&self, table: &str, txn: Txn) -> SqlResult<Box<dyn Iterator<Item = Tuple>>>;
}
