use dashmap::DashMap;

use crate::{
    storage::catalog,
    types::{ItemPointer, Oid, Tx, Visibility, INVALID_OID, INVALID_TXN_ID, MAX_CID},
    TxManager,
};

/// This is Rust translation of optimistic_txn_manager.cpp from Peloton project
pub struct MvOcc {
    a: DashMap<String, String>,
}

impl TxManager for MvOcc {
    fn is_visible(tx: &Tx, tuple_id: Oid) -> Visibility {
        unimplemented!()
        /*
        txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
        cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
        cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);

        bool own = (current_txn->GetTransactionId() == tuple_txn_id);
        bool activated = (current_txn->GetBeginCommitId() >= tuple_begin_cid);
        bool invalidated = (current_txn->GetBeginCommitId() >= tuple_end_cid);

        if (tuple_txn_id == INVALID_TXN_ID) {
          // the tuple is not available.
          if (activated && !invalidated) {
            // deleted tuple
            return VISIBILITY_DELETED;
          } else {
            // aborted tuple
            return VISIBILITY_INVISIBLE;
          }
        }

        // there are exactly two versions that can be owned by a transaction.
        // unless it is an insertion.
        if (own == true) {
          if (tuple_begin_cid == MAX_CID && tuple_end_cid != INVALID_CID) {
            assert(tuple_end_cid == MAX_CID);
            // the only version that is visible is the newly inserted/updated one.
            return VISIBILITY_OK;
          } else if (tuple_end_cid == INVALID_CID) {
            // tuple being deleted by current txn
            return VISIBILITY_DELETED;
          } else {
            // old version of the tuple that is being updated by current txn
            return VISIBILITY_INVISIBLE;
          }
        } else {
          if (tuple_txn_id != INITIAL_TXN_ID) {
            // if the tuple is owned by other transactions.
            if (tuple_begin_cid == MAX_CID) {
              // in this protocol, we do not allow cascading abort. so never read an
              // uncommitted version.
              return VISIBILITY_INVISIBLE;
            } else {
              // the older version may be visible.
              if (activated && !invalidated) {
                return VISIBILITY_OK;
              } else {
                return VISIBILITY_INVISIBLE;
              }
            }
          } else {
            // if the tuple is not owned by any transaction.
            if (activated && !invalidated) {

              return VISIBILITY_OK;
            } else {
              return VISIBILITY_INVISIBLE;
            }
          }
        } */
    }

    fn is_owner(tx: &Tx, tuple_id: Oid) -> bool {
        todo!()
    }

    fn is_ownable(tx: &Tx, tuple_id: Oid) -> bool {
        todo!()
    }

    fn acquire_ownership(tx: &Tx, tuple_id: Oid) -> bool {
        todo!()
    }

    fn yield_ownership(tx: &Tx, tuple_id: Oid) {
        todo!()
    }

    fn perform_read(tx: &Tx, location: ItemPointer) {
        todo!()
    }

    fn perform_insert(tx: &Tx, location: ItemPointer) {
        let tile_group_id = location.block;
        let tuple_id = location.offset;
        let tile_group = catalog::get_tile_group(tile_group_id);
        let tile_group_header = tile_group.get_header();
        let tx_id = tx.id;

        assert_eq!(INVALID_TXN_ID, tile_group_header.borrow().get_tx_id());
        assert_eq!(MAX_CID, tile_group_header.borrow().get_tx_id());
        assert_eq!(MAX_CID, tile_group_header.borrow().get_tx_id());

        tile_group_header.borrow().set_tx_id(tuple_id, tx_id);
        tx.record_insert(location);
    }

    fn perform_update(
        tx: &Tx,
        old_location: ItemPointer,
        new_location: ItemPointer,
        is_blind_write: bool,
    ) {
        todo!()
    }

    fn perform_delete(tx: &Tx, old_location: ItemPointer, new_location: ItemPointer) {
        todo!()
    }

    fn begin_tx() -> Tx {
        todo!()
    }

    fn commit_tx(tx: &Tx) {
        todo!()
    }

    fn abort_tx(tx: &Tx) {
        todo!()
    }
}
