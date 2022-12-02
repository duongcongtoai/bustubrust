## This exist to remind me what i was doing here

### Implementing DataTable::insert_tuple()

- I accept that only Column Layout is supported (no indirection layer)
- Each tile group holds a vector of sub-schema, each of which only holds
  a vector of column
- already implement insert_tuple for DataTable, next is implementing
  InsertExecutor in insert.rs file
- implemented populate_table without tx_manager.perform_insert
### Next steps:
- insert_executor
- the main point is see how the benchmark works

### In the middle of some detail
- insert_executor, case node plan is a children executor
- differentiate between different types of tuple :
  ContainerTuple<LogicalTile>
  storage::Tuple
Value LogicalTile::GetValue(oid_t tuple_id, oid_t column_id) {
  PL_ASSERT(column_id < schema_.size());
  PL_ASSERT(tuple_id < total_tuples_);
  PL_ASSERT(visible_rows_[tuple_id]);

  ColumnInfo &cp = schema_[column_id];
  oid_t base_tuple_id = position_lists_[cp.position_list_idx][tuple_id];
  storage::Tile *base_tile = cp.base_tile.get();

  LOG_TRACE("Tuple : %u Column : %u", base_tuple_id, cp.origin_column_id);
  if (base_tuple_id == NULL_OID) {
    return ValueFactory::GetNullValueByType(
        base_tile->GetSchema()->GetType(column_id));
  } else {
    return base_tile->GetValue(base_tuple_id, cp.origin_column_id);
  }
}


