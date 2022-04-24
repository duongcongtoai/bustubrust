use crate::sql::{exe::Operator, ExecutionContext, SqlResult};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use std::sync::Arc;

use super::exe::SendableDataBlockStream;

pub struct SeqScanPlan {
    pub table: String,
    pub out_schema: Schema,
}

// todo: batch size
#[derive(Debug)]
pub struct SeqScanner {
    predicate: Predicate,
    // ctx: ExecutionContext,
    init: bool,
    table: String,
    schema: SchemaRef,
    // leftover: Option<Box<dyn Iterator<Item = Tuple>>>,
}

#[derive(Debug)]
struct Predicate {}

impl SeqScanner {
    pub fn from_plan(plan: SeqScanPlan, ctx: ExecutionContext) -> Self {
        SeqScanner {
            predicate: Predicate {}, //todo
            // ctx,
            init: false,
            table: plan.table,
            schema: Arc::new(plan.out_schema),
        }
    }
}

#[async_trait::async_trait]
impl Operator for SeqScanner {
    fn execute_sync(&mut self, ctx: ExecutionContext) -> SqlResult<SendableDataBlockStream> {
        todo!()
    }
    async fn execute(&mut self, ctx: ExecutionContext) -> SqlResult<SendableDataBlockStream> {
        ctx.get_storage().scan(&self.table, ctx.get_txn()).await
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
