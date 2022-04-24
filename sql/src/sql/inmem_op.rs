use super::{exe::SchemaStream, DataBlock};
use crate::sql::{
    exe::{Operator, SendableDataBlockStream},
    ExecutionContext, SqlResult,
};
use async_stream::stream;
use async_trait::async_trait;
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

#[async_trait]
impl Operator for InMemOp {
    async fn execute(&mut self, ctx: ExecutionContext) -> SqlResult<SendableDataBlockStream> {
        let batches = self.batches.clone();
        let stream = stream! {
            for item in batches{
                yield Ok(item);
            }
        };
        let schemastream = SchemaStream::new(self.schema.clone(), Box::pin(stream));
        Ok(schemastream)
    }

    fn execute_sync(&mut self, ctx: ExecutionContext) -> SqlResult<SendableDataBlockStream> {
        todo!()
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
