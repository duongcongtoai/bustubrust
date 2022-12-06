use std::cell::RefCell;

use crate::{
    storage::{storage::ProjectInfo, table::DataTable},
    types::Oid,
    TxManager,
};

use super::Executor;

/// There are 2 types of scanning: scanning over a logic expr, or scanning over a table,
/// for simplicity, only impl scanning a table for now
/// and no predicate impl yet ;)
pub struct SeqScan<T: TxManager> {
    tx_manager: T,
    data_table: RefCell<DataTable>,
    project_info: ProjectInfo,
    block_id: Oid, // TODO: it is not supposed to be here :D
    col_ids: Vec<Oid>,
    plan: SeqScanNode,
}

impl<T> SeqScan<T>
where
    T: TxManager,
{
    fn execute(&self) {
        if let Some(child) = self.plan.children {
            unimplemented!()
        }
        Th
    }

    fn _scan_logical_tile() {}
    fn _scan_table() {}
}
pub struct SeqScanNode {
    children: Option<Box<dyn Executor>>,
}
