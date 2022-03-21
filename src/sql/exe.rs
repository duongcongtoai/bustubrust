use super::SqlResult;
use crate::bpm::BufferPoolManager;
use crate::sql::tx::Txn;
use serde_derive::{Deserialize, Serialize};
use std::cmp::Eq;
use std::rc::Rc;

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
    fn insert_tuple(&self, table: &str, tuple: Tuple, txn: Txn) -> SqlResult<RID>;
    fn mark_delete(&self, table: &str, rid: RID, txn: Txn) -> SqlResult<()>;
    fn apply_delete(&self, table: &str, rid: RID, txn: Txn) -> SqlResult<()>;
    fn get_tuple(&self, table: &str, rid: RID, txn: Txn) -> SqlResult<Tuple>;
    fn scan(&self, table: &str, txn: Txn) -> SqlResult<Box<dyn Iterator<Item = Tuple>>>;
}
