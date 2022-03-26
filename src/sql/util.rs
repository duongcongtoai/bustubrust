use super::exe::Operator;
use crate::sql::Batch;
// use crate::sql::ExecutionContext;
use crate::sql::PartialResult;
use crate::sql::{Error, Row};

/// Util struct to create impl of Operator from raw input
pub struct RawInput {
    inner: Vec<Row>,
}

impl Operator for RawInput {
    /* fn from_plan<P>(plan: &P, ctx: ExecutionContext) -> Self
    where
        P: Plan,
    {
        todo!()
    } */
    fn next(&mut self) -> Result<PartialResult, Error> {
        todo!()
    }
}
impl RawInput {
    pub fn new_from_batch(input: Vec<Batch>) -> Self {
        let mut inner = vec![];
        for item in input {
            inner.extend(item.inner);
        }
        RawInput { inner }
    }
    pub fn new(inner: Vec<Row>) -> Self {
        RawInput { inner }
    }
}
