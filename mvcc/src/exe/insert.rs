use crate::storage::table::DataTable;
use std::cell::RefCell;

use crate::{storage::storage::ProjectInfo, types::Tx, ItemPointer, Oid, TxManager};

pub struct Insert<T: TxManager> {
    tx_manager: T,
    data_table: RefCell<DataTable>,
    project_info: ProjectInfo,
    block_id: Oid, // TODO: it is not supposed to be here :D
}

impl<T> Insert<T>
where
    T: TxManager,
{
    /// There are 2 types of insertion: from raw records or from a node that can be executed to
    /// retrieved records
    fn insert_execute(&self, tx: &Tx, tuple: Vec<Oid>) -> bool {
        let schema = self.data_table.borrow().get_schema();
    }
}
