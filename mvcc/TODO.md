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
- insert_executor
- node.GetTuple what is this -> find other usage
- node.GetProjectInfo ?
 has a targetlist, which is a vector of <oid,expression>
 one usage shows oid is col_id, expression can be a constant value
- What does RB stands for -> Stands for Rollback, but why is it a type
  of concurrency protocol???


