use datafusion::prelude::*;
use std::{
    cell::{Ref, RefCell},
    collections::{HashMap, VecDeque},
    io::copy,
    sync::Mutex,
};
use tinyvec::SliceVec;

#[derive(Debug)]
struct Foo;

fn main() {
    let mut ctx = ExecutionContext::new();
    ctx.register_csv("example", "tests/example.csv", CsvReadOptions::new())
        .await?;

    // create a plan
    let df = ctx
        .sql("SELECT a, MIN(b) FROM example GROUP BY a LIMIT 100")
        .await?;

    // execute the plan
    let results: Vec<RecordBatch> = df.collect().await?;

    // format the results
    let pretty_results = arrow::util::pretty::pretty_format_batches(&results)?.to_string();

    let expected = vec![
        "+---+----------------+",
        "| a | MIN(example.b) |",
        "+---+----------------+",
        "| 1 | 2              |",
        "+---+----------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);
}
