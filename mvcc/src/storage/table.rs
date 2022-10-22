use crate::types::Oid;

use super::{storage::ContainerTuple, tile::Schema, tuple::Tuple};

pub struct DataTable {
    id: Oid,
    name: String,
    schema: Schema,
    tuples_per_tilegroup: usize,
}

pub fn populate_table(table: &DataTable, num_row: usize) {
    // something to do with var len pool
    for row in 0..num_row {}

    // location = table->InsertTuple(tuple.get(), &itemptr_ptr);
    // res = transaction_manager.PerformInsert(location);
    // lo
}

impl DataTable {
    pub fn fill_in_empty_tuple_slot(tuple: Tuple) {
        // call gc if there is an available recycled tuple slot -> minor, impl later
        //
    }
}

pub struct ItemPointer {
    block: Oid,
    offset: Oid,
}
