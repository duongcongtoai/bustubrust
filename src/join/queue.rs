use crate::join::grace::Batch;
use crate::join::grace::PartitionedQueue;
use crate::join::grace::Row;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::VecDeque;

pub struct Inmem {
    inner: RefCell<HashMap<usize, VecDeque<Row>>>,
}
impl Inmem {
    pub fn new() -> Self {
        Inmem {
            inner: RefCell::new(HashMap::new()),
        }
    }
}

impl PartitionedQueue for Inmem {
    fn enqueue(&self, partition_idx: usize, data: Vec<Row>) {
        let mut inner = self.inner.borrow_mut();
        match inner.get_mut(&partition_idx) {
            None => {
                let mut new_dequeue = VecDeque::new();
                new_dequeue.extend(data);
                inner.insert(partition_idx, new_dequeue);
            }
            Some(exist) => {
                exist.extend(data);
            }
        }
    }

    fn dequeue(&self, partition_idx: usize, size: usize) -> Option<Batch> {
        let mut inner = self.inner.borrow_mut();
        match inner.get_mut(&partition_idx) {
            None => {
                inner.remove(&partition_idx);
                None
            }
            Some(exist) => {
                let mut ret = Vec::new();
                while let Some(row) = exist.pop_front() {
                    ret.push(row);
                    if ret.len() == size {
                        break;
                    }
                }
                if ret.len() > 0 {
                    Some(Batch::new(ret))
                } else {
                    inner.remove(&partition_idx);
                    None
                }
            }
        }
    }
}
