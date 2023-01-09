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
- need to read paper MVOCC 2011 again and summary into wiki on github
- revisit NSM/DSM/FSM/PAX is a must now
- implemented populate_table without tx_manager.perform_insert
- implementing seqscan, in the middle between is_visible and commit_tx.
- implement TGH to extract tuple begin_ts end end_ts to start writing
  test for visibilty check function
- inside insert_executor, data_table insert receives a pointer of
  pointer and return a location variable, later on the txn manager also
  receives this pointer of pointer to perform insert, what does this
  mean
=> answer: for insert_tuple, the ptr_ptr variable is eventually becomes
the pointer to the location returned by calling
data_table.insert_tuple(). It needs ptr_ptr because during
data_table.insert_tuple, it needs to insert to the index a pointer
pointing to the location of item, in rust, we may store atomicPointer 
inside the index instead, in the tile group header, we also store atomic
pointer, for example from raw pointer, cast that into atomicPointer and
does compare and swap => actually we don't need atomic here, because we
are the only one proccesing for this tuple (not sure about update tho)
so just a cast into *const AtomicPtr<T>, then *a = AtomicPtr<T>. In the
future we may need to read this headPtr and cas some sort up. TBU????
The reason to store pointer of location instead of the raw value of the
locatoin itself, is we only need one single cas operation on this
AtomicPtr to set the header to the new location on Update.

a = some address on the heap: <1,3>
Old tuple: master: Atomic<a>
New Tuple: master: Atomic<a>
What i want to share is AtomicPtr, not the Ptr inside it!!!. We what to
share because we may have multiple index referencing the tuple, and we
only want them to reference to the latest version of the tuple. One
single CAS should do it if we store AtomicPtr. 
TODO: test in a playground crate





