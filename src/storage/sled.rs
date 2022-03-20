use crate::sql::exe::Catalog;
use crate::sql::exe::Schema;
use crate::sql::exe::TableMeta;
use crate::sql::exe::Tuple;
use crate::sql::exe::RID;
use crate::sql::tx::Txn;
use crate::sql::Error;
use crate::sql::SqlResult;
use sled::Db;

pub struct Sled {
    pub tree: Db,
}
impl From<sled::Error> for Error {
    fn from(e: sled::Error) -> Error {
        Error::Value(e.to_string())
    }
}

impl Sled {
    fn new(filename: String) -> SqlResult<Self> {
        let tree = sled::open(filename)?;
        Ok(Sled { tree })
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
impl From<sled::transaction::TransactionError> for Error {
    fn from(e: sled::transaction::TransactionError) -> Error {
        Error::Value(e.to_string())
    }
}

impl Catalog for Sled {
    fn create_table(&self, tablename: String, schema: Schema) -> SqlResult<TableMeta> {
        let table_meta = TableMeta {
            schema,
            oid: 0,
            name: tablename.clone(),
        };
        self.tree.transaction(|tree| {
            let key = format!("schema/{}", tablename);

            let new_scheme = bincode::serialize(&table_meta).expect("serializing schema");

            tree.insert(key.as_bytes(), new_scheme)?;

            Ok(())
        })?;
        Ok(table_meta)
    }
    fn get_table(&self, tablename: String) -> SqlResult<TableMeta> {
        let key = format!("schema/{}", tablename);
        let table_meta: Option<TableMeta> = self
            .tree
            .get(key.as_bytes())?
            .map(|v| bincode::deserialize(&v))
            .transpose()
            .expect("transposing");
        Ok(table_meta.unwrap())
    }
}
#[cfg(test)]
pub mod tests {
    use crate::sql::exe::Catalog;
    use crate::sql::exe::Column;
    use crate::storage::sled::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_sled_catalog() {
        let file = NamedTempFile::new().expect("failed creating temp file");
        let path = file.into_temp_path().to_str().unwrap().to_string();
        let db = Sled::new(path).expect("failed creating sled");

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
            Catalog::create_table(&db, "test_catalog".to_string(), schema).expect("creating table");
        let table_meta = Catalog::get_table(&db, "test_catalog".to_string())
            .expect("getting table test_catalog");

        assert_eq!(ret, table_meta);
    }
}
