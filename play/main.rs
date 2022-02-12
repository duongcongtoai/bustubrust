use std::cell::{Ref, RefCell};
use std::io::copy;
use std::sync::Mutex;
use tinyvec::SliceVec;

#[derive(Debug)]
struct Foo;

fn main() {
    let st = (0..10)
        .into_iter()
        .map(|i| match i % 2 {
            0 => {
                if i % 2 == 0 {
                    return Some(i);
                }
                return None;
            }
            _ => None,
        })
        .filter(|i| i.is_some())
        .map(|some_id| some_id.unwrap());
    println!("{:?}", st);

    let st2 = (0..10).into_iter().filter_map(|i| match i % 2 {
        0 => Some(i),
        _ => None,
    });
    /* let st = (0..2).into_iter();
    let mut st = Fibo {
        name: "1",
        internal: st,
    };
    let mut st2 = Fibo {
        name: "2",
        internal: (0..2).into_iter(),
    };

    st.zip(st2).all(|(a, b)| a == b); */
}

struct Fibo<T: Iterator<Item = i32>> {
    internal: T,
    name: &'static str,
}

// Implement `Iterator` for `Fibonacci`.
// The `Iterator` trait only requires a method to be defined for the `next` element.
impl<T: Iterator<Item = i32>> Iterator for Fibo<T> {
    type Item = i32;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.internal.next();
        println!("processing {:?} {:?}", self.name, next);
        next
    }
}
