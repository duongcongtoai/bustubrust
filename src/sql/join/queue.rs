use crate::sql::join::grace::PartitionedQueue;
use crate::sql::Batch;
use crate::sql::Row;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::rc::Rc;

pub struct Inmem {
    inner: RefCell<HashMap<usize, VecDeque<Row>>>,
    id: usize,
}
pub struct MemoryAllocator {
    queues: Vec<Inmem>,
    cur_id: usize,
}
impl MemoryAllocator {
    pub fn new() -> Self {
        MemoryAllocator {
            queues: vec![],
            cur_id: 0,
        }
    }

    pub fn alloc(&mut self) -> Rc<Inmem> {
        let cur_id = self.cur_id;
        self.cur_id += 1;
        let new = Inmem::new(cur_id);
        let rc = Rc::new(new);
        return rc;
    }
}
impl Inmem {
    pub fn new(id: usize) -> Self {
        Inmem {
            inner: RefCell::new(HashMap::new()),
            id,
        }
    }
}

impl PartitionedQueue for Inmem {
    fn id(&self) -> usize {
        self.id
    }
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
