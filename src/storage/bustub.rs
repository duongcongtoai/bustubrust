use crate::bpm::BufferPoolManager;
use crate::bpm::Frame;
use crate::bpm::PAGE_SIZE;
use crate::sql;
use crate::sql::exe::Catalog;
use crate::sql::exe::Schema;
use crate::sql::exe::Storage;
use crate::sql::exe::TableMeta;
use crate::sql::exe::Tuple;
use crate::sql::exe::RID;
use crate::sql::tx::Txn;
use crate::sql::SqlResult;
use bytemuck::try_from_bytes;
use bytemuck::try_from_bytes_mut;
use core::cell::RefCell;
use core::mem::size_of;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

pub struct Bustub {
    bpm: BufferPoolManager,
    table_ref: HashMap<String, u32>,
    // tables: Vec<TableMeta>,
    catalogs: CatalogJson,
    catalog_frame: Option<Arc<Mutex<Frame>>>,
    next_oid: u32,
}

#[derive(Serialize, Deserialize)]
pub struct CatalogJson {
    size: usize,
    tables: Vec<TableMeta>,
}

impl Bustub {
    fn new(bpm: BufferPoolManager) -> SqlResult<Self> {
        let mut ret = Bustub {
            catalogs: CatalogJson {
                size: 0,
                tables: vec![],
            },
            catalog_frame: None,
            table_ref: HashMap::new(),
            next_oid: 0,
            bpm,
        };
        ret.init()?;
        Ok(ret)
    }
    fn init(&mut self) -> SqlResult<()> {
        if self.bpm.dm.file_size()? < PAGE_SIZE as u64 {
            let header_frame = self.bpm.new_page().expect("failed to create new page");
            let mut locked_header = header_frame.lock();
            if locked_header.get_page_id() != 0 {
                return Err(sql::Error::Value("header page id != 0".to_string()));
            }
            let raw = locked_header.get_raw_data();
            let raw_json = r#"{"size":0,"tables":[]}"#.to_string();
            raw[..raw_json.len()].clone_from_slice(raw_json.as_bytes());
            // flush empty byte to this file region
            self.bpm.flush_locked(&mut locked_header)?;
        }

        match self.bpm.fetch_page(0) {
            Ok(header_frame) => {
                let mut locked_header = header_frame.lock();
                let raw = locked_header.get_raw_data();
                let some_str = String::from_utf8(raw.to_vec()).unwrap();
                let trimmed = some_str.trim_matches(char::from(0));
                let ret: CatalogJson = serde_json::from_str(trimmed)?;
                self.catalogs = ret;
                let mut max_oid: i32 = -1;
                for item in &self.catalogs.tables {
                    self.table_ref.insert(item.name.clone(), item.oid);
                    if item.oid as i32 > max_oid {
                        max_oid = item.oid as i32;
                    }
                }
                drop(locked_header);
                self.catalog_frame = Some(header_frame);
                match max_oid < 0 {
                    true => self.next_oid = 0,
                    false => self.next_oid = max_oid as u32,
                }
                Ok(())
            }
            Err(some_err) => Err(sql::Error::Value(format!("todo: {:?}", some_err))),
        }
    }
    fn update_catalog_json(&self, new_json: String) -> SqlResult<()> {
        let mut locked_header = self.catalog_frame.as_ref().unwrap().lock();
        let raw = locked_header.get_raw_data();

        // let (raw_header, next) = raw.split_at_mut(size_of::<usize>());
        if new_json.len() > raw.len() {
            sql::Error::Value("new json overflow catalog page size".to_string());
        }
        raw[..new_json.len()].clone_from_slice(new_json.as_bytes());
        for item in raw[new_json.len()..].iter_mut() {
            *item = 0;
        }
        /* let catalog_size = try_from_bytes_mut::<usize>(raw_header).unwrap();
         *catalog_size = new_json.len(); */

        Ok(())
    }
    fn insert_tuple(&self, table: &str, tuple: Tuple, rid: RID, txn: Txn) -> SqlResult<()> {
        Ok(())
    }
    fn mark_delete(&self, table: &str, rid: RID, txn: Txn) -> SqlResult<()> {
        Ok(())
    }
    fn apply_delete(&self, table: &str, rid: RID, txn: Txn) -> SqlResult<()> {
        Ok(())
    }
}

/* impl Storage for RefCell<Bustub> {
    fn insert_tuple(&self, table: &str, tuple: Tuple, rid: RID, txn: Txn) -> SqlResult<()> {
        Ok(())
    }
    fn mark_delete(&self, table: &str, rid: RID, txn: Txn) -> SqlResult<()> {
        Ok(())
    }
    fn apply_delete(&self, table: &str, rid: RID, txn: Txn) -> SqlResult<()> {
        Ok(())
    }
    fn get_tuple(&self, table: &str, rid: RID, txn: Txn) -> SqlResult<Tuple> {
        Ok(())
    }
    fn scan(&self, table: &str, txn: Txn) -> SqlResult<Box<dyn Iterator<Item = Tuple>>> {
        Ok(())
    }
} */
impl Catalog for RefCell<Bustub> {
    fn create_table(&self, tablename: String, schema: Schema) -> SqlResult<TableMeta> {
        let mut s = self.borrow_mut();
        match s.table_ref.get(&tablename) {
            Some(_) => {
                return Err(sql::Error::Value(
                    format!("table {} already exist", tablename).to_string(),
                ));
            }
            None => {}
        }
        let oid = s.next_oid;
        s.table_ref.insert(tablename.clone(), oid);
        let ret = TableMeta {
            schema,
            name: tablename,
            oid,
        };
        // s.table_ref[&tablename] = oid;
        s.catalogs.tables.push(ret.clone());
        s.update_catalog_json(serde_json::to_string(&s.catalogs)?)?;

        s.next_oid += 1;
        Ok(ret)
    }
    fn get_table(&self, tablename: String) -> SqlResult<TableMeta> {
        let s = self.borrow();
        for item in &s.catalogs.tables {
            if item.name == tablename {
                return Ok(item.clone());
            }
        }
        return Err(sql::Error::Value("not found table".to_string()));
    }
}
#[cfg(test)]
pub mod tests {
    use crate::bpm::DiskManager;
    use crate::replacer::LRURepl;
    use crate::sql::exe::Catalog;
    use crate::sql::exe::Column;
    use crate::storage::bustub::BufferPoolManager;
    use crate::storage::bustub::Bustub;
    use crate::storage::bustub::Schema;
    use crate::storage::bustub::PAGE_SIZE;
    use core::cell::RefCell;
    use tempfile::tempfile;

    #[test]
    fn test_catalog() {
        let pool_size = 10;
        let dm = DiskManager::new_from_file(tempfile().unwrap(), PAGE_SIZE as u64);
        let repl = LRURepl::new(pool_size);
        let bpm = BufferPoolManager::new(pool_size, Box::new(repl), dm);
        let bt = RefCell::new(Bustub::new(bpm).expect("creating bustub instance"));

        let mut columns = vec![];

        columns.push(Column::new(
            "id".to_string(),
            crate::sql::exe::DataType::INTEGER,
        ));
        columns.push(Column::new(
            "some_bool".to_string(),
            crate::sql::exe::DataType::BOOL,
        ));
        let schema = Schema { columns };
        let ret =
            Catalog::create_table(&bt, "test_catalog".to_string(), schema).expect("creating table");
        let table_meta = Catalog::get_table(&bt, "test_catalog".to_string())
            .expect("getting table test_catalog");
        assert_eq!(ret, table_meta);
    }
}
