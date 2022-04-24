use async_stream::{stream, try_stream, AsyncStream};
use comfy_table::{Cell, Table, TableComponent};
use datafusion::{
    arrow::{error::Result, record_batch::RecordBatch, util::display::array_value_to_string},
    error::Result,
};
// use core::task::{Context, Poll};
use futures::stream::StreamExt;
use futures_core::Stream;
use std::{iter::IntoIterator, pin::Pin};
use strum::IntoEnumIterator;

#[derive(Debug)]
struct Foo;

#[tokio::main]
async fn main() {
    let batch =RecordBatch::try_new(schema, columns)

    let st = create_table(vec![
        vec!["col_a", "col_b", "col_c"],
        vec!["val_1", "val_b", "val_ccc"],
    ]);
    println!("{}", st);
}

fn create_pretty_print_table(batches: &[RecordBatch]) -> Result<Table> {
    /* let expected = vec![
        "+----+----+----+----+----+----+",
        "| a1 | b1 | c1 | a2 | b1 | c2 |",
        "+----+----+----+----+----+----+",
        "| 1  | 4  | 7  | 10 | 4  | 70 |",
        "| 2  | 5  | 8  | 20 | 5  | 80 |",
        "| 3  | 5  | 9  | 20 | 5  | 80 |",
        "+----+----+----+----+----+----+",
    ]; */
    let schema = batches[0].schema();

    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row_cells = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let column = batch.column(col_idx);
                let val_str = array_value_to_string(column, row_idx)?;
                row_cells.push(Cell::new(val_str));
            }
            table.add_row(row_cells);
        }
    }
    Ok(table)
}
