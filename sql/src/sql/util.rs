use std::{
    ops::{Generator, GeneratorState},
    pin::Pin,
};

use super::{exe::BoxedDataIter, SqlResult};
use crate::sql::DataBlock;
use comfy_table::{Cell, Table};
use datafusion::arrow::util::display::array_value_to_string;
// use futures::TryStreamExt;

/// Util struct to create impl of Operator from raw input
pub struct RawInput {
    inner: DataBlock,
}

/// Create a vector of record batches from a stream
pub fn collect(stream: BoxedDataIter) -> SqlResult<Vec<DataBlock>> {
    stream.collect()
    // stream.try_collect::<Vec<_>>().await
}

pub fn create_pretty_print_table(batches: &[DataBlock]) -> SqlResult<Table> {
    let schema = batches[0].schema();

    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");
    if batches.is_empty() {
        return Ok(table);
    }

    let schema = batches[0].schema();
    let mut header = Vec::new();
    for field in schema.fields() {
        header.push(Cell::new(&field.name()));
    }
    table.set_header(header);

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

#[macro_export]
macro_rules! assert_batches_sorted_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
        let mut expected_lines: Vec<String> = $EXPECTED_LINES.iter().map(|&s| s.into()).collect();

        // sort except for header + footer
        let num_lines = expected_lines.len();
        if num_lines > 3 {
            expected_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }
        let formatted = crate::sql::util::create_pretty_print_table($CHUNKS)
            .unwrap()
            .to_string();

        let mut actual_lines: Vec<&str> = formatted.trim().lines().collect();

        // sort except for header + footer
        let num_lines = actual_lines.len();
        if num_lines > 3 {
            actual_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }

        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    };
}

pub struct GeneratorIteratorAdapter<G>(Pin<Box<G>>);

impl<G> GeneratorIteratorAdapter<G>
where
    G: Generator<Return = ()>,
{
    pub fn new(gen: G) -> Self {
        Self(Box::pin(gen))
    }
}

impl<G> Iterator for GeneratorIteratorAdapter<G>
where
    G: Generator<Return = ()>,
{
    type Item = G::Yield;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.as_mut().resume(()) {
            GeneratorState::Yielded(x) => Some(x),
            GeneratorState::Complete(_) => None,
        }
    }
}
