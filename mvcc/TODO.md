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
- txmanager.perform_insert
- executor_test_util.cpp also uses this method, maybe trying that
- the main point is see how the benchmark works
