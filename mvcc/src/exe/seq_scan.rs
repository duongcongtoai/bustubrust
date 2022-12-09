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
            // scan logical tuple (result gathered from another excutor)
            unimplemented!()
        }
        // scan from a table
    }

    fn _scan_logical_tile(&self) {}
    fn _scan_table(&self) {
        //
        // for each tile_group = table table group
        //
        // local vars:
        // - position_list: a list of
        //
        // for each tuple in tilegroup
        // item = Position{block: tilegroup_id, offset: tuple_id}
        // if TxManager::is_visible(tile_group_header,tuple_id) {
        //  evaluate predicate if any
        //  if tx is not staticreadonly => TxManager::PerformRead(location)
        //  tldr: PerformRead record such location has already been read by tx, so in validation
        //  phase, it validate if such location is still visible
        //
        //  use tilegroup obj and position list to create logical tile (it typically hold ref to
        //  the underhood tiles and the position list)
        // }
    }
}
pub struct SeqScanNode {
    children: Option<Box<dyn Executor>>,
}
