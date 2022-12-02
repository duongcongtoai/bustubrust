use crate::{
    storage::{table::DataTable, tuple::Tuple},
    types::INVALID_OID,
};
use std::cell::RefCell;

use crate::{storage::storage::ProjectInfo, types::Tx, Oid, TxManager};

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
        let loc = self.data_table.borrow().insert_tuple(tuple);
        if loc.block == INVALID_OID {
            panic!("tx failed")
        }
        T::perform_insert(tx, loc);
        return true;
    }
}
pub struct InsertPlan {
    tuple: Option<Tuple>,
    // children_node:
}
