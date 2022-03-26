use crate::sql::exe::Operator;
use crate::sql::exe::Tuple;
use crate::sql::ExecutionContext;
use crate::sql::PartialResult;
use crate::sql::Row;
use crate::sql::SqlResult;
use itertools::Itertools;

pub struct SeqScanPlan {
    table: String,
}

// todo: batch size
pub struct SeqScanner {
    predicate: Predicate,
    ctx: ExecutionContext,
    init: bool,
    table: String,
    leftover: Option<Box<dyn Iterator<Item = Tuple>>>,
}

struct Predicate {}
impl SeqScanner {
    pub fn from_plan(plan: SeqScanPlan, ctx: ExecutionContext) -> Self {
        SeqScanner {
            predicate: Predicate {}, //todo
            ctx,
            init: false,
            table: plan.table,
            leftover: None,
        }
    }
}

impl Operator for SeqScanner {
    fn next(&mut self) -> SqlResult<PartialResult> {
        if !self.init {
            self.leftover = Some(
                self.ctx
                    .get_storage()
                    .scan(&self.table, self.ctx.get_txn())?,
            );
            self.init = true;
        }

        let rows: Vec<Row> = vec![];
        let st = self.leftover.as_mut().unwrap();

        for chunk in &st.chunks(10) {
            let mut rows = Vec::new();
            for item in chunk {
                rows.push(Row::new(item.data));
            }
        }
        if rows.len() == 0 {
            Ok(PartialResult::new_done())
        } else {
            Ok(PartialResult::new(rows))
        }
    }
}
