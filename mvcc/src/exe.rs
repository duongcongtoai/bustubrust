use std::cell::RefCell;

use crate::{types::Tx, ItemPointer, Oid, TxManager};

pub struct Update<T: TxManager> {
    tx_manager: T,
    data_table: RefCell<DataTable>,
    project_info: ProjectInfo,
    block_id: Oid, // TODO: it is not supposed to be here :D
}

/// TODO: read more on this paper: https://15721.courses.cs.cmu.edu/spring2018/papers/06-mvcc2/p677-neumann.pdf
/// for now, a tuple_id represents physical pointer to the version of the tuple
impl<T> Update<T>
where
    T: TxManager,
{
    /// TODO: what is the input here:
    /// - and output of another operator
    /// - does this output represents a sequence of abstracted tuple_id
    /// - or does it represent physicial position of a sequence of tuples
    /// => we choose physical tuple_id for now (the one that include the versin)
    ///
    /// - case tx already own the lock on tuple
    /// - case tuple is ownablew the lock on tuple
    /// -- case success:
    /// -- case failure:
    fn update_execute(&self, tx: &Tx, tuple_ids: Vec<Oid>) -> bool {
        for tuple_id in tuple_ids {
            if T::acquire_ownership(tx, tuple_id) {
                // TODO
            } else if T::is_ownable(tx, tuple_id) {
                if !T::acquire_ownership(tx, tuple_id) {
                    log::trace!(
                        "failed to acquire ownership on tuple {}, aborting txn {}",
                        tuple_id,
                        tx.id,
                    );
                    return false;
                }
                let mut data_table = self.data_table.borrow_mut();
                let new_location = data_table.acquire_version();
                let new_tuple = data_table.get_data_tuple(new_location);

                let old_location = ItemPointer::new(self.block_id, tuple_id);
                let old_tuple = data_table.get_data_tuple(old_location);
                self.project_info.evaluate_single(new_tuple, old_tuple);

                T::perform_update(tx, old_location, new_location, false);
                // TODO: how to recognize conflict here
            }
        }
        true
    }
}
pub struct DataTable {}

impl DataTable {
    fn get_data_tuple(&mut self, block_id: ItemPointer) -> ContainerTuple {
        unimplemented!()
    }

    // allocate a new region in memory (datablock) and return location of that memory region/data
    // block
    fn acquire_version(&mut self) -> ItemPointer {
        unimplemented!()
    }

    // this method is related to index management, complex logic related to secondary index,
    // depends on the type (type_tuple, type_version), need to read in the paper more
    fn install_version(
        &mut self,
        tuple: ContainerTuple,
        location: ItemPointer,
        target_list: Vec<Target>,
        master_ptr: ItemPointer,
    ) -> bool {
        unimplemented!()
    }
}
pub struct ProjectInfo {
    target_list: Vec<Target>,
}
impl ProjectInfo {
    fn evaluate_single(&self, dest: ContainerTuple, t1: ContainerTuple) -> bool {
        false
    }
    fn evaluate_double(
        &self,
        dest: ContainerTuple,
        t1: ContainerTuple,
        t2: ContainerTuple,
    ) -> bool {
        false
    }
}
pub struct Target {
    col_id: Oid,
    expr: Expression,
}

pub struct Expression {}

impl Expression {
    fn evaluate(t1: ContainerTuple, t2: ContainerTuple) -> Value {
        unimplemented!()
    }
}
/// Hold pointer to memory region of the underlying tuple
/// used for inplace update
pub struct ContainerTuple {}
pub struct Value {}
