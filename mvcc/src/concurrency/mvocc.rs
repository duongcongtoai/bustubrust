use dashmap::DashMap;
use lazy_static::lazy_static;

use crate::{
    storage::{catalog, tile::TileGroupHeader},
    types::{ItemPointer, Oid, Tx, TxPhase, Visibility, CID, INVALID_TXN_ID, MAX_CID},
    TxManager,
};

/// This is Rust translation of optimistic_txn_manager.cpp from Peloton project
pub struct MvOcc {
    a: DashMap<String, String>,
}
lazy_static! {
    // key is tx_id, value is state of tx as well as its ts (begin or commit ts,depends on state)
    static ref TX_STATE : DashMap<CID,(TxPhase,CID)> = {
        let mut m = DashMap::new();
        m
    };
}

pub fn is_ts(id: CID) -> bool {
    id.leading_zeros() > 0
}

impl MvOcc {
    fn _extract_tuple_end_ts(
        tx: &Tx,
        read_ts: CID,
        tuple_id: Oid,
        tgh: &TileGroupHeader,
    ) -> (CID, Visibility, bool) {
        let t_end_ts = tgh.get_tuple_begin_ts(tuple_id);
        let mut visible = false;
        let mut speculative_result = false;
        let mut return_cid = 0;
        if is_ts(t_end_ts) {
            visible = read_ts < t_end_ts;
            return_cid = t_end_ts;
        } else {
            let owning_tx_id = t_end_ts & u64::MAX >> 1;
            let (tx_state, tx_ts) = *TX_STATE.get(&owning_tx_id).unwrap();
            return_cid = tx_ts;
            match tx_state {
                Processing => {
                    // other txn is attempting to delete this version, but not committed
                    if tx.id != owning_tx_id {
                        return_cid = 0;
                        visible = true;
                    }
                    // most likely this version is created, and later on deleted within the same txn
                    visible = false
                }
                Preparing => {
                    // even if this tx abort, future txn removing this tuple will always happen
                    // after read_ts
                    if read_ts < tx_ts {
                        visible = true
                    }
                    // speculative ignore, tx must commit for this result to be consistent
                    speculative_result = true;
                    visible = false;
                }
                Committed => {
                    visible = read_ts < tx_ts;
                }
                Aborted => {
                    visible = true;
                    return_cid = 0;
                }
            }
        }
        if visible {
            return (return_cid, Visibility::Visible, speculative_result);
        }

        return (return_cid, Visibility::Deleted, speculative_result);
    }
    // return (lower bound ts, whether this ts is visible, whether it is speculative read)
    fn _extract_tuple_begin_ts(
        tx: &Tx,
        read_ts: CID,
        tuple_id: Oid,
        tgh: &TileGroupHeader,
    ) -> (CID, Visibility, bool) {
        let return_cid = 0;
        let t_bgin_ts = tgh.get_tuple_begin_ts(tuple_id);
        let mut speculative_result = false;
        let mut visible = false;
        if is_ts(t_bgin_ts) {
            visible = read_ts > t_bgin_ts;
            return_cid = t_bgin_ts;
        } else {
            let owning_tx_id = t_bgin_ts & u64::MAX >> 1;
            let (tx_state, tx_ts) = *TX_STATE.get(&owning_tx_id).unwrap();
            return_cid = tx_ts;
            match tx_state {
                Processing => {
                    // tx_ts is begin ts
                    visible = read_ts > tx_ts && tx.id == owning_tx_id;
                }
                Preparing => {
                    visible = read_ts > tx_ts;
                    speculative_result = true;
                }
                Committed => {
                    visible = read_ts > tx_ts;
                }
                Aborted => {
                    return_cid = 0;
                    visible = false;
                }
            }
        }
        if visible {
            return (return_cid, Visibility::Visible, speculative_result);
        }
        return (return_cid, Visibility::Invisible, speculative_result);
    }
}

impl TxManager for MvOcc {
    fn is_visible(tx: &Tx, tuple_id: Oid, tgh: &TileGroupHeader) -> Visibility {
        // determine read time == tx's time or current time
        //
        // TODO: determine this read ts
        let read_ts = 0;
        let t_bgin_ts = tgh.get_tuple_begin_ts(tuple_id);
        let (_, begin_ts_visible, speculative_read) =
            MvOcc::_extract_tuple_begin_ts(tx, read_ts, tuple_id, tgh);
        match begin_ts_visible {
            Invisible => return Visibility::Invisible,
            Deleted => panic!("begin_ts_visibility cannot has result 'deleted'"),
            _ => {}
        }
        let (_, end_ts_visible, speculative_ignore) =
            MvOcc::_extract_tuple_end_ts(tx, read_ts, tuple_id, tgh);
        match end_ts_visible {
            Visibility => return Visibility::Visible,
            Deleted => return Visibility::Deleted,
            _ => panic!("end_ts_visibility cannot has result 'invisible'"),
        }
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

    /// install its tx_id into the location, if fails, we must abort
    /// also, set headptr = index_addr in the header of this tuple
    /// the head ptr is useful when we update a tuple, we allocate another addr to store the new
    /// value of a given tuple, and we want to set the head ptr to this new location, we can do
    /// this if we share a ptr to the value of this addr (which can also shared in the index as
    /// well)
    fn perform_insert_with_index_ptr(tx: &Tx, location: ItemPointer, index_addr: &mut ItemPointer) {
        let tile_group_id = location.block;
        let tuple_id = location.offset;
        let tile_group = catalog::get_tile_group(tile_group_id);
        let tile_group_header = tile_group.get_header();
        let tx_id = tx.id;

        // assert_eq!(INVALID_TXN_ID, tile_group_header.borrow().get_tx_id());
        /* assert_eq!(MAX_CID, tile_group_header.borrow().get_tx_id());
        assert_eq!(MAX_CID, tile_group_header.borrow().get_tx_id()); */

        let success = tile_group_header
            .borrow()
            .install_owning_tx(tuple_id, tx_id);
        if !success {
            panic!("todo")
        }
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
