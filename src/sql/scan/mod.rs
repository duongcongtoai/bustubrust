use std::sync::Arc;

use crate::sql::{exe::Operator, ExecutionContext, Schema, SqlResult};
use arrow::datatypes::SchemaRef;
use itertools::Itertools;

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

struct Predicate {}
impl SeqScanner {
    pub fn from_plan(plan: SeqScanPlan, ctx: ExecutionContext) -> Self {
        SeqScanner {
            predicate: Predicate {}, //todo
            // ctx,
            init: false,
            table: plan.table,
            schema: Arc::new(plan.schema),
        }
    }
}

#[async_trait::async_trait]
impl Operator for SeqScanner {
    async fn execute(&mut self, ctx: ExecutionContext) -> SqlResult<SendableDataBlockStream> {
        self.ctx
            .get_storage()
            .scan(&self.table, ctx.get_txn())?
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
