use crate::sql::{
    exe::{BoxedDataIter, SchemaStream},
    join::grace::PartitionedQueue,
    DataBlock, SqlResult,
};
use datafusion::arrow::datatypes::SchemaRef;
use parking_lot::Mutex;
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    sync::Arc,
    task::{Context, Poll},
};

#[derive(Debug)]
pub struct Inmem {
    inner: RefCell<HashMap<usize, VecDeque<DataBlock>>>,
    id: usize,
    schema: SchemaRef,
}

unsafe impl Sync for RawMemoryAllocator {}
pub struct MemoryAllocator {
    inner: Mutex<RawMemoryAllocator>,
}

pub struct RawMemoryAllocator {
    queues: Vec<Inmem>,
    cur_id: usize,
}
impl MemoryAllocator {
    pub fn new() -> Self {
        MemoryAllocator {
            inner: Mutex::new(RawMemoryAllocator::new()),
        }
    }

    pub fn alloc(&self, schema: SchemaRef) -> Arc<Inmem> {
        self.inner.lock().alloc(schema)
    }
}
impl RawMemoryAllocator {
    pub fn new() -> Self {
        RawMemoryAllocator {
            queues: vec![],
            cur_id: 0,
        }
    }

    pub fn alloc(&mut self, schema: SchemaRef) -> Arc<Inmem> {
        let cur_id = self.cur_id;
        self.cur_id += 1;
        let new = Inmem::new(cur_id, schema);
        let rc = Arc::new(new);
        return rc;
    }
}
impl Inmem {
    pub fn new(id: usize, schema: SchemaRef) -> Self {
        Inmem {
            inner: RefCell::new(HashMap::new()),
            id,
            schema,
        }
    }
}
pub struct DequeueFut {
    all: VecDeque<DataBlock>,
}

unsafe impl Send for DequeueFut {}
unsafe impl Sync for DequeueFut {}

impl Iterator for DequeueFut {
    fn next(&mut self) -> Option<Self::Item> {
        match self.all.pop_front() {
            None => None,
            Some(record) => Some(Ok(record)),
        }
    }
    type Item = SqlResult<DataBlock>;
}

unsafe impl Send for Inmem {}
unsafe impl Sync for Inmem {}

impl PartitionedQueue for Inmem {
    fn id(&self) -> usize {
        self.id
    }
    fn enqueue(&self, partition_idx: usize, data: DataBlock) -> SqlResult<()> {
        let mut inner = self.inner.borrow_mut();
        match inner.get_mut(&partition_idx) {
            None => {
                let mut new_dequeue = VecDeque::new();
                new_dequeue.push_back(data);
                inner.insert(partition_idx, new_dequeue);
            }
            Some(exist) => {
                exist.push_back(data);
            }
        }
        Ok(())
    }

    fn dequeue_all(&self, partition_idx: usize) -> SqlResult<DataBlock> {
        let mut inner = self.inner.borrow_mut();
        let ret = DataBlock::new_empty(self.schema.clone());
        match inner.get_mut(&partition_idx) {
            None => Err(format!(
                "not found data for partition idx {}",
                partition_idx
            ))?,
            Some(exist) => {
                let mut st = vec![];
                while let Some(batch) = exist.pop_front() {
                    st.push(batch);
                }
                let st = DataBlock::concat(&self.schema, &st)?;
                Ok(st)
            }
        }
    }

    fn dequeue(&self, partition_idx: usize, size: usize) -> SqlResult<BoxedDataIter> {
        let mut inner = self.inner.borrow_mut();
        match inner.remove(&partition_idx) {
            None => Err(format!("partition {} does not exist", partition_idx))?,
            Some(exist) => {
                let fut = DequeueFut { all: exist };
                Ok(SchemaStream::new(self.schema.clone(), Box::new(fut)))
            }
        }
    }
}
