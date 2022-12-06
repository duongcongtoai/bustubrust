## This exist to remind me what i was doing here

### Implementing DataTable::insert_tuple()

- I accept that only Column Layout is supported (no indirection layer)
- Each tile group holds a vector of sub-schema, each of which only holds
  a vector of column
- already implement insert_tuple for DataTable, next is implementing
  InsertExecutor in insert.rs file
- implemented populate_table without tx_manager.perform_insert
### Current goals:
Deeply understands how query executor works including:
- seq scan
- index scan
- bitmap heap scan
- Join: merge join/hash join/grace hash join 


### Next steps:
- the main point is see how the benchmark works
- impl seq_scan
- impl test for seq_scan
- impl test for tile/tile_group
- impl test for insert_executor

### In the middle of some detail
- revisit NSM/DSM/FSM/PAX is a must now
- implemented populate_table without tx_manager.perform_insert
- what is the procesing method used by Peloton (iterator/materialization/Vectorized)


