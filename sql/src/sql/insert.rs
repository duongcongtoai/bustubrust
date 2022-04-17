use super::exe::Operator;
use super::exe::PlanType;
// use crate::sql::exe::Executor;
// use crate::sql::exe::Tuple;
// use crate::sql::Error;
use crate::sql::ExecutionContext;
// use crate::sql::PartialResult;

pub struct InsertPlan {
    pub table: String,
    pub source_plan: Box<PlanType>,
}
pub struct Insert {
    table: String,
    source: Box<dyn Operator>,
    ctx: ExecutionContext,
}

// impl Insert {
//     pub fn new(plan: InsertPlan, ctx: ExecutionContext) -> Self {
//         let source = Executor::create_operator(*plan.source_plan, ctx.clone());
//         Insert {
//             table: plan.table,
//             source,
//             ctx,
//         }
//     }
// }

// impl Operator for Insert {
//     fn next(&mut self) -> Result<PartialResult, Error> {
//         let storage = self.ctx.get_storage();
//         let ret = self.source.next()?;
//         if ret.done {
//             return Ok(PartialResult::new_done());
//         }
//         let tuples = Tuple::new_multi(ret.inner);
//         storage.insert_tuples(&self.table, tuples, &self.ctx.txn)?;
//         Ok(PartialResult::new(vec![]))
//     }
// }
