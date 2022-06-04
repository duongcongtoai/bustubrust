use super::{exe::SchemaStream, util::GeneratorIteratorAdapter, DataBlock};
use crate::sql::{
    exe::{BoxedDataIter, Operator},
    ExecutionContext, SqlResult,
};
use datafusion::arrow::datatypes::SchemaRef;

#[derive(Debug)]
pub struct InMemOp {
    schema: SchemaRef,
    batches: Vec<DataBlock>,
    // cur_batch: usize,
}
impl InMemOp {
    pub fn new(schema: SchemaRef, batches: Vec<DataBlock>) -> Self {
        InMemOp { schema, batches }
    }
}

impl Operator for InMemOp {
    fn execute_sync(&mut self, ctx: ExecutionContext) -> SqlResult<BoxedDataIter> {
        let batches = self.batches.clone();
        let stream = || {
            for item in batches {
                yield Ok(item);
            }
        };
        let iter = GeneratorIteratorAdapter::new(stream);
        let schemastream = SchemaStream::new(self.schema.clone(), Box::new(iter));
        Ok(schemastream)
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
