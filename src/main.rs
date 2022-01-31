use bustubrust::bpm::DiskManager;
use std::cell::UnsafeCell;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Barrier};
use std::thread::{sleep, spawn};
use std::time;
use std::{
    os::unix::thread,
    sync::atomic::{AtomicBool, AtomicI64, Ordering},
};

// use cc::read;

static my_num: i64 = 0;
static has_data: AtomicBool = AtomicBool::new(false);
static NTHREADS: usize = 8;

fn main() {
    main2();
}

#[derive(Default)]
struct UsizePair {
    atomic: AtomicUsize,
    normal: UnsafeCell<usize>,
}

static NITERS: usize = 1000000;
/* struct SendPtr<T>(*const T);
unsafe impl<T> Sync for SendPtr<T> {}
unsafe impl<T> Send for SendPtr<T> {} */

unsafe impl Sync for UsizePair {}
impl UsizePair {
    fn get(&self) -> (usize, usize) {
        let atom = self.atomic.load(Ordering::Relaxed); //Ordering::Acquire

        let norm = unsafe { *self.normal.get() };
        (atom, norm)
    }
    pub fn set(&self, v: usize) {
        unsafe { *self.normal.get() = v };

        self.atomic.store(v, Ordering::Relaxed); //Ordering::Release
    }
}

fn main2() {
    /* let shared2 = shared.clone();
    let shared3 = shared.clone(); */

    loop {
        let barrier = Arc::new(Barrier::new(NTHREADS + 1));
        let shared = Arc::new(UsizePair::default());
        let mut children = vec![];

        for _ in 0..NTHREADS {
            let shared = shared.clone();
            let barrier = barrier.clone();
            children.push(spawn(move || {
                barrier.wait();
                let mut v = 0;
                while v < NITERS - 1 {
                    let (atom, norm) = shared.get();
                    if atom > norm {
                        println!("reordered !!!! {} > {}", atom, norm)
                    }
                    v = atom;
                }
            }));
        }
        barrier.wait();
        for v in 1..NITERS {
            shared.set(v);
        }
        for child in children {
            child.join().unwrap();
        }

        // has_data.store(false, Ordering::SeqCst);
        sleep(time::Duration::from_millis(50));
        println!("retying");
    }
}

fn do_something() -> i32 {
    let a = 1;
    a + 2
}
