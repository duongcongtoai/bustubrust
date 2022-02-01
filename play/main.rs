use owning_ref;
use std::cell::{Ref, RefCell};
use std::io::copy;
use std::sync::Mutex;
use tinyvec::SliceVec;

#[derive(Debug)]
struct Foo;

fn main() {
    let mut st = [0; 10];
    for i in 0..10 {
        st[i] = i;
    }

    let mut st2 = &mut st;
    let mut st3 = SliceVec::from_slice_len(st2, 9);
    st3.insert(1, 3);
    // let mut st3 = st2.to_vec();
    // st3.insert(1, 3);
    // st3.push(1);
    println!("{:?}", st);
}
