use crate::bpm::BufferPoolManager;
use crate::sql::exe::Catalog;
use crate::sql::exe::Schema;
use crate::sql::exe::Storage;
use crate::sql::exe::TableMeta;
use crate::sql::exe::Tuple;
use crate::sql::exe::RID;
use crate::sql::tx::Txn;
use crate::sql::{Error, SqlResult};
use core::cell::RefCell;
use sled;
use std::collections::HashMap;
use tinyvec::SliceVec;
/* pub struct Bustub<'a> {
    bpm: BufferPoolManager,
    rootfile: String,
    table_ref: HashMap<String,usize>,
    tables: SliceVec<'a, TableMeta>, // must be memory mapped
}

impl <'a>Bustub<'a> {}

impl <'a>Storage for RefCell<Bustub<'a>> {
    fn insert_tuple(&self, table: &str, tuple: Tuple, rid: RID, txn: Txn) -> SqlResult<()> {}
    fn mark_delete(&self, table: &str, rid: RID, txn: Txn) -> SqlResult<()> {}
    fn apply_delete(&self, table: &str, rid: RID, txn: Txn) -> SqlResult<()> {}
    fn get_tuple(&self, table: &str, rid: RID, txn: Txn) -> SqlResult<Tuple> {}
    fn scan(&self, table: &str, txn: Txn) -> SqlResult<Box<dyn Iterator<Item = Tuple>>> {}
}
impl <'a>Catalog for RefCell<Bustub<'a>> {
    fn create_table(&self, tablename: String, schema: Schema) -> Result<TableMeta, Error> {
        self.borrow().bpm.fetch_page(0)
        let mut tablefile = self.borrow().rootfile.clone();
        tablefile.push_str(&tablename);

        let thistable = sled::open(tablefile)?;
        let ret = TableMeta {
            schema,
            name: tablename,
            oid: 0,
        };

        self.borrow_mut().table_files[&tablename] = (thistable, ret.clone());
        Ok(ret)
    }
    fn get_table(&self, tablename: String) -> Result<TableMeta, Error> {
        Ok(self.borrow().table_files[&tablename].1.clone())
    }
} */
