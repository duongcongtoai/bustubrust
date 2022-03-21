use crate::sql::exe::Catalog;
use crate::sql::exe::Schema;
use crate::sql::exe::Storage;
use crate::sql::exe::TableMeta;
use crate::sql::exe::Tuple;
use crate::sql::exe::RID;
use crate::sql::tx::Txn;
use crate::sql::Error;
use crate::sql::SqlResult;
use bytemuck::try_from_bytes;
use sled::Db;
use sled::IVec;
use std::array::TryFromSliceError;
use std::convert::TryInto;
use zerocopy::AsBytes;

pub struct Sled {
    pub tree: Db,
}
impl From<sled::Error> for Error {
    fn from(e: sled::Error) -> Error {
        Error::Value(e.to_string())
    }
}
impl From<TryFromSliceError> for Error {
    fn from(e: TryFromSliceError) -> Error {
        Error::Value(e.to_string())
    }
}

impl Sled {
    fn new(filename: String) -> SqlResult<Self> {
        let tree = sled::open(filename)?;
        Ok(Sled { tree })
    }

    /* fn mark_delete(&self, table: &str, rid: RID, txn: Txn) -> SqlResult<()> {
        Ok(())
    }
    fn apply_delete(&self, table: &str, rid: RID, txn: Txn) -> SqlResult<()> {
        Ok(())
    } */
}
impl From<sled::transaction::TransactionError> for Error {
    fn from(e: sled::transaction::TransactionError) -> Error {
        Error::Value(e.to_string())
    }
}
impl From<&Tuple> for IVec {
    fn from(tuple: &Tuple) -> IVec {
        let mut ret = IVec::from(vec![0; 8 + tuple.data.len()]);
        ret[..8].clone_from_slice(&tuple.rid.to_be_bytes());
        ret[8..].clone_from_slice(&tuple.data);
        ret
    }
}

impl Storage for Sled {
    fn insert_tuple(&self, table: &str, tuple: Tuple, txn: Txn) -> SqlResult<RID> {
        let tuple_ref = &tuple;
        let rid = self.tree.transaction(move |tree| {
            let rid = tree.generate_id()?;
            let id = rid.to_be_bytes();
            let prefix = format!("data/{}/", table.to_string());

            let key_bytes = prefix
                .into_bytes()
                .into_iter()
                .chain(id.iter().copied())
                .collect::<Vec<_>>();
            tree.insert(IVec::from(key_bytes), tuple_ref)?;
            tree.flush();
            Ok(rid)
        })?;
        Ok(rid as RID)
    }
    fn mark_delete(&self, table: &str, tuple: RID, txn: Txn) -> SqlResult<()> {
        /* self.tree.transaction(move |tree|{
            tree.remove(key)

        }) */
        todo!()
    }
    fn apply_delete(&self, _: &str, _: RID, _: Txn) -> SqlResult<()> {
        todo!()
    }
    fn get_tuple(&self, _: &str, _: RID, _: Txn) -> SqlResult<Tuple> {
        todo!()
    }
    // Scan all table
    fn scan(&self, table: &str, txn: Txn) -> SqlResult<Box<(dyn Iterator<Item = Tuple>)>> {
        let prefix = format!("data/{}", table.to_string());
        let ret: Result<Vec<Tuple>, Error> = self
            .tree
            .scan_prefix(prefix.as_bytes())
            .map(move |item| {
                let (key, value) = item.expect("scanning item");
                let rid: RID = RID::from_be_bytes(key.as_bytes().try_into()?);
                return Ok(Tuple::construct(rid, value.to_vec()));
            })
            .collect();
        Ok(Box::new(ret?.into_iter()))
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
            tree.flush();
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
