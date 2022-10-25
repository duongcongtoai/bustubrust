use crate::types::{ItemPointer, Oid};

use super::manager::StorageManager;

pub struct DataTable {}

impl DataTable {
    pub fn new(storage: &StorageManager) -> Self {
        unimplemented!()
    }
    /// This obj abstracts a huge memory region, it will return the memory segment of the given
    /// tuple_id, but abstracted inside a ContainerTuple to allow inplace update
    pub fn get_data_tuple(&mut self, tuple_id: ItemPointer) -> ContainerTuple {
        unimplemented!()
    }

    // allocate a new region in memory (datablock) and return location of that memory region/data
    // block
    pub fn acquire_version(&mut self) -> ItemPointer {
        unimplemented!()
    }

    // this method is related to index management, complex logic related to secondary index,
    // depends on the type (type_tuple, type_version), need to read in the paper more
    pub fn install_version(
        &mut self,
        tuple: ContainerTuple,
        location: ItemPointer,
        target_list: Vec<Target>,
        master_ptr: ItemPointer,
    ) -> bool {
        unimplemented!()
    }
}
/// Hold pointer to memory region of the underlying tuple
/// used for inplace update
#[derive(Clone)]
pub struct ContainerTuple {}
pub struct Value {}

pub struct ProjectInfo {
    target_list: Vec<Target>,
}
impl ProjectInfo {
    pub fn evaluate_inplace(&self, dest: ContainerTuple) -> bool {
        false
    }
    pub fn evaluate_single(&self, dest: ContainerTuple, t1: ContainerTuple) -> bool {
        false
    }
    fn evaluate_double(
        &self,
        dest: ContainerTuple,
        t1: ContainerTuple,
        t2: ContainerTuple,
    ) -> bool {
        false
    }
}
pub struct Target {
    col_id: Oid,
    expr: Expression,
}

pub struct Expression {}

impl Expression {
    fn evaluate(t1: ContainerTuple, t2: ContainerTuple) -> Value {
        unimplemented!()
    }
}
