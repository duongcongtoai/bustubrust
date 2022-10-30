use crate::storage::table::DataTable;
use std::cell::RefCell;

use crate::{storage::storage::ProjectInfo, types::Tx, ItemPointer, Oid, TxManager};

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
        /* for tuple_id in tuple_ids {
            // This happens when the previous executor has already made some change to this tuple
            // and in this executor, we make change to it again, then we only need to update the
            // version previous created by the previous executor
            if T::is_owner(tx, tuple_id) {
                let mut data_table = self.data_table.borrow_mut();
                let old_location = ItemPointer::new(self.block_id, tuple_id);
                let old_tuple = data_table.get_data_tuple(old_location);
                self.project_info.evaluate_inplace(old_tuple);

                // TODO: i don't know if other special mvcc impl has other logic to deal inside
                // txmanager or not. For MVOCC, it only need this. maybe just directly call tx's method for now
                // tx hold a list of updated item, must announce this to it
                tx.record_update(old_location);
            // TODO
            } else if T::is_ownable(tx, tuple_id) {
                // some other tx has alread hold write lock on this tx, abort
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
                // TODO: logic related to index mgmt

                T::perform_update(tx, old_location, new_location, false);
            }
        }
        true */
        unimplemented!()
    }
}
