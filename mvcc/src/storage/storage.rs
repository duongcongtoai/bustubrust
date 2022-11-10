use crate::types::Oid;

use super::tuple::Value;

/// Hold pointer to memory region of the underlying tuple
/// used for inplace update
#[derive(Clone)]
pub struct ContainerTuple {}

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
