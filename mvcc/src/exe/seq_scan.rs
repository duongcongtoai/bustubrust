use std::cell::RefCell;

use crate::{
    storage::{storage::ProjectInfo, table::DataTable},
    types::Oid,
    TxManager,
};

/// There are 2 types of scanning: scanning over a logic expr, or scanning over a table,
/// for simplicity, only impl scanning a table for now
/// and no predicate impl yet ;)
pub struct SeqScan<T: TxManager> {
    tx_manager: T,
    data_table: RefCell<DataTable>,
    project_info: ProjectInfo,
    block_id: Oid, // TODO: it is not supposed to be here :D
    col_ids: Vec<Oid>,
    // predicate: Sometype,
}

impl<T> SeqScan<T>
where
    T: TxManager,
{
    fn execute() {
        /* LOG_TRACE("Seq Scan executor :: 0 child ");

        PL_ASSERT(target_table_ != nullptr);
        PL_ASSERT(column_ids_.size() > 0);

        // Force to use occ txn manager if dirty read is forbidden
        concurrency::TransactionManager &transaction_manager =
            concurrency::TransactionManagerFactory::GetInstance();

        // LOG_TRACE("Number of tuples: %f",
        // target_table_->GetIndex(0)->GetNumberOfTuples());

        // Retrieve next tile group.
        while (current_tile_group_offset_ < table_tile_group_count_) {
          auto tile_group =
              target_table_->GetTileGroup(current_tile_group_offset_++);
          auto tile_group_header = tile_group->GetHeader();

          oid_t active_tuple_count = tile_group->GetNextTupleSlot();


          // Construct position list by looping through tile group
          // and applying the predicate.
          std::vector<oid_t> position_list;
          for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {

            ItemPointer location(tile_group->GetTileGroupId(), tuple_id);
            LOG_TRACE("Seq scanning tuple (%u, %u)", location.block, location.offset);

            // check transaction visibility
            if (transaction_manager.IsVisible(tile_group_header, tuple_id) == VISIBILITY_OK) {
              LOG_TRACE("Seq scan on visible tuple (%u, %u)", location.block, location.offset);

              // if the tuple is visible, then perform predicate evaluation.
              if (predicate_ == nullptr) {
                position_list.push_back(tuple_id);

                if (concurrency::current_txn->IsStaticReadOnlyTxn() == false) {
                  auto res = transaction_manager.PerformRead(location);
                  if (!res) {
                    transaction_manager.SetTransactionResult(RESULT_FAILURE);
                    return res;
                  }
                }

              } else {
                expression::ContainerTuple<storage::TileGroup> tuple(
                    tile_group.get(), tuple_id);
                auto eval = predicate_->Evaluate(&tuple, nullptr, executor_context_)
                                .IsTrue();
                if (eval == true) {
                  position_list.push_back(tuple_id);

                  if (concurrency::current_txn->IsStaticReadOnlyTxn() == false) {
                    auto res = transaction_manager.PerformRead(location);
                    if (!res) {
                      transaction_manager.SetTransactionResult(RESULT_FAILURE);
                      return res;
                    }
                  }
                }
              }
            }
          }

          // Don't return empty tiles
          if (position_list.size() == 0) {
            continue;
          }

          // Construct logical tile.
          std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
          logical_tile->AddColumns(tile_group, column_ids_);
          logical_tile->AddPositionList(std::move(position_list));

          SetOutput(logical_tile.release());
          return true;
        } */
    }
}
