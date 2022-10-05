pub mod commit;
pub mod mvocc;
pub mod types;

pub trait TxManager {
    // Visibility check
    // check whether a tuple is visible to current transaction.
    // in this protocol, we require that a transaction cannot see other
    // transaction's local copy.
    fn is_visible(tuple_id: u64) -> Visibility {
        Visibility::Invisible
    }

    // check whether the current transaction owns the tuple.
    // this function is called by update/delete executors.
    fn is_owner(tuple_id: u64) -> bool {
        false
    }

    // if the tuple is not owned by any transaction and is visible to current
    // transaction.
    // this function is called by update/delete executors.
    fn is_ownable(tuple_id: u64) -> bool {
        false
    }

    // get write lock on a tuple.
    // this is invoked by update/delete executors.
    fn acquire_ownership(tile_group_id: u64, tuple_id: u64) -> bool {
        false
    }

    // release write lock on a tuple.
    // one example usage of this method is when a tuple is acquired, but operation
    // (insert,update,delete) can't proceed, the executor needs to yield the
    // ownership before return false to upper layer.
    // It should not be called if the tuple is in the write set as commit and abort
    // will release the write lock anyway.
    fn yield_ownership(tile_group_id: u64, tuple_id: u64) {}

    fn perform_read(location: ItemPointer) {}
    fn perform_insert(location: ItemPointer) {}
    fn perform_update(old_location: ItemPointer, new_location: ItemPointer, is_blind_write: bool) {}
    fn perform_delete(old_location: ItemPointer, new_location: ItemPointer) {}

    fn commit_tx() {}
    fn abort_tx() {}
}
pub struct ItemPointer {
    block: u64,
    offset: u64,
}
pub enum Visibility {
    Invisible,
    Deleted,
    Visible,
}
