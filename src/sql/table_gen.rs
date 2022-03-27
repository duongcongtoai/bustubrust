use super::{
    exe::{DataType, ExecutionContext, Schema, Tuple},
    SqlResult,
};
use crate::sql::exe::Column;
use crate::sql::tx::Txn;
use crate::sql::Row;
use itertools::Itertools;
use rand::distributions::{Distribution, Uniform};
use serde_derive::{Deserialize, Serialize};
use std::{any::Any, cmp::min};

struct GenTableUtil {
    ctx: ExecutionContext,
}

#[derive(Serialize, Deserialize, Debug)]
struct TableMeta {
    name: String,
    size: usize,
    cols: Vec<ColMeta>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ColMeta {
    #[serde(flatten)]
    schema: Column,

    nullable: bool,
    dist: Dist,
    min: usize,
    max: usize,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
enum Dist {
    Uniform,
    Serial,
}

impl GenTableUtil {
    fn gen_integers(col: &ColMeta, count: usize) -> Vec<Row> {
        let mut values = vec![];
        if col.dist == Dist::Serial {
            for i in 0..count as i32 {
                let st = i.to_be_bytes();
                values.push(Row::new(st.to_vec()));
            }
            return values;
        }

        let between = Uniform::from(col.min as i32..col.max as i32);
        let mut rng = rand::thread_rng();
        for i in 0..count as i32 {
            let sample = between.sample(&mut rng).to_be_bytes();
            values.push(Row::new(sample.to_vec()));
        }
        return values;
    }
    fn make_values(col: &ColMeta, count: usize) -> Vec<Row> {
        match col.schema.type_id {
            DataType::INTEGER => Self::gen_integers(col, count),
            _ => {
                todo!()
            }
        }
    }
    fn gen(&self, file: String) -> SqlResult<()> {
        let store = self.ctx.get_storage();
        let json_str = std::fs::read_to_string(file).unwrap();
        let table_meta: TableMeta = serde_json::from_str(&json_str).unwrap();
        let mut col = vec![];
        for item in &table_meta.cols {
            col.push(Column::new(item.schema.name.clone(), item.schema.type_id));
        }

        let batch_size = 128;

        store.create_table(table_meta.name.clone(), Schema { columns: col })?;
        let mut inserted = 0;
        while inserted < table_meta.size {
            // seed values for columns
            let mut values_by_column = vec![];
            let num_val = min(batch_size, inserted);
            for item in &table_meta.cols {
                values_by_column.push(Self::make_values(item, num_val));
            }
            let mut batched_tuples = vec![];

            for row_idx in 0..num_val {
                let mut entry: Vec<u8> = vec![];
                for single_column_rows in &values_by_column {
                    let this_column_data = &single_column_rows[row_idx].inner;
                    entry.copy_from_slice(this_column_data);
                }
                batched_tuples.push(Tuple::new(entry));
            }
            store.insert_tuples(&table_meta.name, batched_tuples, &Txn {})?;
            inserted += num_val;
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use crate::sql::exe::Column;
    use crate::sql::exe::DataType;
    use crate::sql::table_gen::ColMeta;
    use crate::sql::table_gen::Dist;
    use crate::sql::table_gen::TableMeta;

    #[test]
    fn test_json() {
        let some_json = r#"
        {
            "name": "test_1",
            "size": 1000,
            "cols": [
                {
                    "name": "colA",
                    "type_id": "INTEGER",
                    "nullable": false,
                    "dist": "Serial",
                    "min": 0,
                    "max": 0
                }
            ]
        }"#;
        let got: TableMeta = serde_json::from_str(some_json).unwrap();
        let got_col = &got.cols[0];
        let expect = TableMeta {
            name: "test_1".to_string(),
            size: 1000,
            cols: vec![ColMeta {
                schema: Column::new("colA".to_string(), DataType::INTEGER),
                nullable: false,
                dist: Dist::Serial,
                min: 0,
                max: 0,
            }],
        };
        let expect_col = &expect.cols[0];
        assert_eq!(expect.name, got.name);
        assert_eq!(expect.size, got.size);
        assert_eq!(expect_col.schema.name, got_col.schema.name);
        assert_eq!(expect_col.schema.type_id, got_col.schema.type_id);
        assert_eq!(expect_col.nullable, got_col.nullable);
        assert_eq!(expect_col.dist, got_col.dist);
        assert_eq!(expect_col.min, got_col.min);
        assert_eq!(expect_col.max, got_col.max);
    }
}
