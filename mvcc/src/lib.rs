use types::{ItemPointer, Oid, Tx, Visibility};

// pub mod commit; this is a playground lib, do not used
pub mod concurrency;
pub mod exe;
pub mod mvocc;
pub mod storage;
pub mod types;

pub trait TxManager {
    // Visibility check
    // check whether a tuple is visible to current transaction.
    // in this protocol, we require that a transaction cannot see other
    // transaction's local copy.
    fn is_visible(tx: &Tx, tuple_id: Oid) -> Visibility;

    // check whether the current transaction owns the tuple.
    // this function is called by update/delete executors.
    fn is_owner(tx: &Tx, tuple_id: Oid) -> bool;

    // if the tuple is not owned by any transaction and is visible to current
    // transaction.
    // this function is called by update/delete executors.
    fn is_ownable(tx: &Tx, tuple_id: Oid) -> bool;

    // get write lock on a tuple.
    // this is invoked by update/delete executors.
    fn acquire_ownership(tx: &Tx, tuple_id: Oid) -> bool;

    // release write lock on a tuple.
    // one example usage of this method is when a tuple is acquired, but operation
    // (insert,update,delete) can't proceed, the executor needs to yield the
    // ownership before return false to upper layer.
    // It should not be called if the tuple is in the write set as commit and abort
    // will release the write lock anyway.
    fn yield_ownership(tx: &Tx, tuple_id: Oid);

    fn perform_read(tx: &Tx, location: ItemPointer);
    fn perform_insert(tx: &Tx, location: ItemPointer);
    fn perform_update(
        tx: &Tx,
        old_location: ItemPointer,
        new_location: ItemPointer,
        is_blind_write: bool,
    );
    fn perform_delete(tx: &Tx, old_location: ItemPointer, new_location: ItemPointer);

    fn begin_tx() -> Tx;
    fn commit_tx(tx: &Tx);
    fn abort_tx(tx: &Tx);
}
