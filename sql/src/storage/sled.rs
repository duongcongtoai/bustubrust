use crate::sql::{
    exe::{BoxedDataIter, Catalog, Storage},
    tx::Txn,
    util, Error, SqlResult,
};
use sled::{Db, IVec};
use std::{array::TryFromSliceError, convert::TryInto};

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
    pub fn new(filename: String) -> SqlResult<Self> {
        let tree = sled::open(filename)?;
        Ok(Sled { tree })
    }
}
/* impl From<sled::transaction::TransactionError> for Error {
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
 */

impl Storage for Sled {
    fn get_tuples(&self, table: &str, rids: BoxedDataIter, txn: &Txn) -> SqlResult<BoxedDataIter> {
        todo!()
    }

    fn scan(&self, table: &str, txn: &Txn) -> SqlResult<BoxedDataIter> {
        todo!()
    }

    fn insert_tuples(
        &self,
        table: &str,
        data: BoxedDataIter,
        txn: &Txn,
    ) -> SqlResult<BoxedDataIter> {
        todo!()
        /* let datablocks = util::collect(data).await?;
        let res = tokio::task::spawn_blocking(move || {
            let rid = self.tree.transaction(move |tree| {
                let rid = tree.generate_id()?;
                let id = rid.to_be_bytes();
                for item in &tuples {
                    let prefix = format!("data/{}/", table.to_string());
                    let key_bytes = prefix
                        .into_bytes()
                        .into_iter()
                        .chain(id.iter().copied())
                        .collect::<Vec<_>>();
                    tree.insert(IVec::from(key_bytes), item.data.clone())?;
                }

                tree.flush();
                Ok(rid)
            })?;
        })
        .await?;
        // let tuple_ref = &tuple;
        let rid = self.tree.transaction(move |tree| {
            let rid = tree.generate_id()?;
            let id = rid.to_be_bytes();
            for item in &tuples {
                let prefix = format!("data/{}/", table.to_string());
                let key_bytes = prefix
                    .into_bytes()
                    .into_iter()
                    .chain(id.iter().copied())
                    .collect::<Vec<_>>();
                tree.insert(IVec::from(key_bytes), item.data.clone())?;
            }

            tree.flush();
            Ok(rid)
        })?;
        Ok(rid as RID) */
    }

    fn delete(&self, table: &str, data: BoxedDataIter, txn: &Txn) -> SqlResult<()> {
        todo!()
    }
}

impl Catalog for Sled {}

#[cfg(test)]
pub mod tests {

    #[test]
    fn test_sled_catalog() {}

    /// Insert a bunch, then seq scan and compare values
    /// Don't care about order (using hashmap)
    #[test]
    fn test_sled_raw_insert() {}
}
