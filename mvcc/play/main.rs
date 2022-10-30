use mvcc::storage::block::{self, RawBlock};

fn main() {
    println!("{}", std::mem::align_of::<RawBlock>());
    block::block_data_size_db();
}
