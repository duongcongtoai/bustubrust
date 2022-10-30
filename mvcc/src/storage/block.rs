use std::{mem::size_of, sync::atomic::AtomicUsize};

use parking_lot::RawMutex;

use super::table::DataTable;

// 1 mb aligned
#[repr(align(1024))]
pub struct RawBlock<'a> {
    data_table: &'a DataTable,
    insert_head: AtomicUsize,
    access_controller: RawMutex,
    // don't know if we need padding or not
    content: [u8; block_data_size()],
}
pub fn block_data_size_db() {
    println!("{}", size_of::<&DataTable>());
    println!("{}", size_of::<AtomicUsize>());
    println!("{}", size_of::<RawMutex>());
}

pub const fn block_data_size() -> usize {
    1024 - size_of::<&DataTable>() - size_of::<AtomicUsize>() - size_of::<RawMutex>()
}
