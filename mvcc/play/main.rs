use std::{sync::atomic::AtomicPtr, thread::spawn};

use mvcc::storage::block::{self, RawBlock};

/// TODO: assigning multiple tx with write-write conflict
fn main() {
    let mut data = 10;
    let mut atomic_ptr = AtomicPtr::new(&mut data);
    let mut other_data_1 = 5;
    let mut other_data_2 = 6;
    let t1 = spawn(move || {
        *atomic_ptr.get_mut() = &mut other_data_1;
    });
    let t2 = spawn(move || {
        *atomic_ptr.get_mut() = &mut other_data_2;
    });
    t1.join().unwrap();
    t2.join().unwrap();
}
