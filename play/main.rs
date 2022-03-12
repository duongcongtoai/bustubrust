use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::copy;
use std::sync::Mutex;
use tinyvec::SliceVec;

#[derive(Debug)]
struct Foo;

fn main() {
    let mut st = RefCell::new(HashMap::new());
    let mut vd = VecDeque::new();
    vd.push_back(1);
    st.borrow_mut().insert("a", vd);
    let st2 = st.borrow_mut().get_mut("a").unwrap().pop_front().unwrap();
    println!("{}", st2);
    let st2 = st.borrow_mut().get_mut("a").unwrap().pop_front().unwrap();
    println!("{}", st2);
}
