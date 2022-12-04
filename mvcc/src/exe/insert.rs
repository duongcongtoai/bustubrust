use crate::{
    storage::{table::DataTable, tuple::Tuple},
    types::INVALID_OID,
};
use std::cell::RefCell;

use crate::{storage::storage::ProjectInfo, types::Tx, Oid, TxManager};

use super::Executor;

pub struct Insert<T: TxManager> {
    plan: InsertPlan,
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
        let projection_inf = self.project_info;
        // case insert tile_group
        if let Some(child) = self.plan.children_node {
            child.execute();
            let logical_tile = child.get_output();
            let t = Tuple::new(&schema);
            for logical_tuple_id in logical_tile.into_iter() {
                for col_id in 0..schema.get_column_count() {
                    let value = logical_tile.get_value(logical_tuple_id, col_id as u32);
                    t.set_value(col_id as u32, value)
                }
                // storage part
                let loc = self.data_table.borrow().insert_tuple(t);
                if loc.block == INVALID_OID {
                    panic!("tx failed")
                }
                // mvcc part
                T::perform_insert(tx, loc);
            }

            return false;
        }

        // case insert physical tuple
        let tuple = match self.plan.tuple {
            Some(t) => t,
            None => {
                let t = Tuple::new(&schema);
                for target in projection_inf.target_list {
                    let val = target.const_evaluate();
                    t.set_value(target.col_id, val);
                }
                t
            }
        };
        // storage part
        let loc = self.data_table.borrow().insert_tuple(tuple);
        if loc.block == INVALID_OID {
            panic!("tx failed")
        }
        // mvcc part
        T::perform_insert(tx, loc);
        return true;
    }
}
pub struct InsertPlan {
    tuple: Option<Tuple>,
    children_node: Option<Box<dyn Executor>>,
}
