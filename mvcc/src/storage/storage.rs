use crate::types::Oid;

use super::tuple::Value;

/// Hold pointer to memory region of the underlying tuple
/// used for inplace update
#[derive(Clone)]
pub struct ContainerTuple {}

pub struct ProjectInfo {
    pub target_list: Vec<Target>,
}
impl Target {
    pub fn evaluate_inplace(&self, dest: ContainerTuple) -> bool {
        false
    }
    pub fn const_evaluate(&self) -> Value {
        unimplemented!()
    }
    pub fn evaluate_single(&self, dest: ContainerTuple, t1: ContainerTuple) -> Value {
        unimplemented!()
    }
    fn evaluate_double(
        &self,
        dest: ContainerTuple,
        t1: ContainerTuple,
        t2: ContainerTuple,
    ) -> Value {
        unimplemented!()
    }
}
pub struct Target {
    pub col_id: Oid,
    expr: Expression,
}

pub struct Expression {}

impl Expression {
    fn evaluate(t1: ContainerTuple, t2: ContainerTuple) -> Value {
        unimplemented!()
    }
}
