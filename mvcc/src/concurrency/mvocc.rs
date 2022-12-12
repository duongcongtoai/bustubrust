use dashmap::DashMap;

use crate::{
    storage::catalog,
    types::{ItemPointer, Oid, Tx, Visibility, INVALID_TXN_ID, MAX_CID},
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
        tx.record_read(location);
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
        // get commit_id
        // in occ, this commitID decides the order of txn
        //
        // for each tilegroup in rwset
        //
        // for each tuple in set
        // case the tuple is not new
        // - if tuple is own by current txn, continue
        // - if tuple is not own by any txn, and its end_commit_id > our_end_commit_id => still
        // visible
        /* LOG_TRACE("Committing peloton txn : %lu ", current_txn->GetTransactionId());

        auto &manager = catalog::Manager::GetInstance();

        auto &rw_set = current_txn->GetRWSet();

        // we can optimize read-only transaction.


        // generate transaction id.
        cid_t end_commit_id = GetNextCommitId();

        // validate read set.
        for (auto &tile_group_entry : rw_set) {
          oid_t tile_group_id = tile_group_entry.first;
          auto tile_group = manager.GetTileGroup(tile_group_id);
          auto tile_group_header = tile_group->GetHeader();
          for (auto &tuple_entry : tile_group_entry.second) {
            auto tuple_slot = tuple_entry.first;
            // if this tuple is not newly inserted.
            if (tuple_entry.second != RW_TYPE_INSERT &&
                tuple_entry.second != RW_TYPE_INS_DEL) {
              // if this tuple is owned by this txn, then it is safe.
              if (tile_group_header->GetTransactionId(tuple_slot) ==
                  current_txn->GetTransactionId()) {
                // the version is owned by the transaction.
                continue;
              } else {
                if (tile_group_header->GetTransactionId(tuple_slot) ==
                        INITIAL_TXN_ID && tile_group_header->GetBeginCommitId(
                                              tuple_slot) <= end_commit_id &&
                    tile_group_header->GetEndCommitId(tuple_slot) >= end_commit_id) {
                  // the version is not owned by other txns and is still visible.
                  continue;
                }
              }
              LOG_TRACE("transaction id=%lu",
                        tile_group_header->GetTransactionId(tuple_slot));
              LOG_TRACE("begin commit id=%lu",
                        tile_group_header->GetBeginCommitId(tuple_slot));
              LOG_TRACE("end commit id=%lu",
                        tile_group_header->GetEndCommitId(tuple_slot));
              // otherwise, validation fails. abort transaction.
              return AbortTransaction();
            }
          }
        }
        //////////////////////////////////////////////////////////

        // auto &log_manager = logging::LogManager::GetInstance();
        // log_manager.LogBeginTransaction(end_commit_id);
        // install everything.
        for (auto &tile_group_entry : rw_set) {
          oid_t tile_group_id = tile_group_entry.first;
          auto tile_group = manager.GetTileGroup(tile_group_id);
          auto tile_group_header = tile_group->GetHeader();
          for (auto &tuple_entry : tile_group_entry.second) {
            auto tuple_slot = tuple_entry.first;
            if (tuple_entry.second == RW_TYPE_UPDATE) {
              // logging.
              ItemPointer new_version =
                  tile_group_header->GetPrevItemPointer(tuple_slot);
              //ItemPointer old_version(tile_group_id, tuple_slot);

              // logging.
              // log_manager.LogUpdate(current_txn, end_commit_id, old_version,
              //                      new_version);

              // we must guarantee that, at any time point, AT LEAST ONE version is
              // visible.
              // we do not change begin cid for old tuple.
              auto new_tile_group_header =
                  manager.GetTileGroup(new_version.block)->GetHeader();

              new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);
              new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                      end_commit_id);

              COMPILER_MEMORY_FENCE;

              tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);

              COMPILER_MEMORY_FENCE;

              new_tile_group_header->SetTransactionId(new_version.offset,
                                                      INITIAL_TXN_ID);
              tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

              // GC recycle.
              RecycleOldTupleSlot(tile_group_id, tuple_slot, end_commit_id);

            } else if (tuple_entry.second == RW_TYPE_DELETE) {
              ItemPointer new_version =
                  tile_group_header->GetPrevItemPointer(tuple_slot);
              ItemPointer delete_location(tile_group_id, tuple_slot);

              // logging.
              // log_manager.LogDelete(end_commit_id, delete_location);

              // we do not change begin cid for old tuple.
              auto new_tile_group_header =
                  manager.GetTileGroup(new_version.block)->GetHeader();

              new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);
              new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                      end_commit_id);

              COMPILER_MEMORY_FENCE;

              tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);

              COMPILER_MEMORY_FENCE;

              new_tile_group_header->SetTransactionId(new_version.offset,
                                                      INVALID_TXN_ID);
              tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

              // GC recycle.
              RecycleOldTupleSlot(tile_group_id, tuple_slot, end_commit_id);

            } else if (tuple_entry.second == RW_TYPE_INSERT) {
              assert(tile_group_header->GetTransactionId(tuple_slot) ==
                     current_txn->GetTransactionId());
              // set the begin commit id to persist insert
              ItemPointer insert_location(tile_group_id, tuple_slot);
              // log_manager.LogInsert(current_txn, end_commit_id, insert_location);

              tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
              tile_group_header->SetBeginCommitId(tuple_slot, end_commit_id);

              COMPILER_MEMORY_FENCE;

              tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

            } else if (tuple_entry.second == RW_TYPE_INS_DEL) {
              assert(tile_group_header->GetTransactionId(tuple_slot) ==
                     current_txn->GetTransactionId());

              // set the begin commit id to persist insert
              tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
              tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);

              COMPILER_MEMORY_FENCE;

              tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);
            }
          }
        }
        // log_manager.LogCommitTransaction(end_commit_id);

        EndTransaction();

        return Result::RESULT_SUCCESS; */
    }

    fn abort_tx(tx: &Tx) {
        todo!()
    }
}
