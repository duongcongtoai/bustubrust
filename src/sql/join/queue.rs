use crate::sql::{
    exe::{SchemaStream, SendableDataBlockStream},
    join::grace::PartitionedQueue,
    DataBlock, Schema, SqlResult,
};
use async_stream::try_stream;
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    pin::Pin,
    rc::Rc,
};

#[derive(Debug)]
pub struct Inmem {
    inner: RefCell<HashMap<usize, VecDeque<DataBlock>>>,
    id: usize,
    schema: Schema,
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

    pub fn alloc(&mut self, schema: Schema) -> Rc<Inmem> {
        let cur_id = self.cur_id;
        self.cur_id += 1;
        let new = Inmem::new(cur_id, schema);
        let rc = Rc::new(new);
        return rc;
    }
}
impl Inmem {
    pub fn new(id: usize, schema: Schema) -> Self {
        Inmem {
            inner: RefCell::new(HashMap::new()),
            id,
            schema,
        }
    }
}

unsafe impl Send for Inmem {}
unsafe impl Sync for Inmem {}

#[async_trait::async_trait]
impl PartitionedQueue for Inmem {
    fn id(&self) -> usize {
        self.id
    }
    async fn enqueue(&self, partition_idx: usize, data: DataBlock) -> SqlResult<()> {
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
        Ok(())
    }

    async fn dequeue_all(&self, partition_idx: usize) -> SqlResult<DataBlock> {
        let mut inner = self.inner.borrow_mut();
        let ret = DataBlock::new_empty(self.schema.clone());
        match inner.get_mut(&partition_idx) {
            None => Err("not found".to_string()),
            Some(exist) => {
                let st = DataBlock::concat(self.schema, exist.pop_front().iter());
                st
            }
        }
    }

    async fn dequeue(
        &self,
        partition_idx: usize,
        size: usize,
    ) -> SqlResult<SendableDataBlockStream> {
        let raw_stream = try_stream! {
            loop{
                let mut inner = self.inner.borrow_mut();
                match inner.get_mut(&partition_idx) {
                    None => {
                        inner.remove(&partition_idx);
                        return;
                    }
                    Some(exist) => {
                        let mut ret = Vec::new();
                        while let Some(row) = exist.pop_front() {
                            yield Ok(row);
                        }

                        inner.remove(&partition_idx);
                        return;
                    }
                }
            }
        };
        Ok(SchemaStream::new(
            self.schema.clone(),
            Pin::pin(Box::new(raw_stream)),
        ))
    }
}
