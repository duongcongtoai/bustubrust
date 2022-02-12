use crate::sql::Value;
use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::sql::executor::Executor;
use crate::sql::tx::Transaction;
use crate::sql::ResultSet;

pub struct HashInnerJoin<T: Transaction> {
    left: Box<dyn Executor<T>>,
    left_field: usize, // index of the joined field inside the tuple of left table
    right: Box<dyn Executor<T>>,
    right_field: usize, // index of the joined field inside the tuple of the right table
    outer: bool,
}
impl<T: Transaction> Executor<T> for HashInnerJoin<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        if let ResultSet::Query {
            rows: outer_rows,
            columns: mut outer_columns,
        } = self.left.execute(txn)?
        {
            let (l, r) = (self.left_field, self.right_field);
            if let ResultSet::Query {
                rows: right_rows,
                columns: right_columns,
            } = self.right.execute(txn)?
            {
                let inner_table: HashMap<Value, Vec<Value>> = right_rows
                    .map(|r_tuple| match r_tuple {
                        Ok(values) => {
                            if values.len() <= r {
                                return Err(Error::Internal(
                                    "right index out of bound".to_string(),
                                ));
                            }
                            let joined_key = values[r].clone();
                            return Ok((joined_key, values));
                        }
                        Err(err) => Err(err),
                    })
                    .collect::<Result<_>>()?;
                outer_columns.extend(right_columns);
                // iterate outer rows, probe inner table, if match omit, else do nothing
                // this is like an inner join
                let joined_rows = outer_rows.filter_map(move |tuple_result| match tuple_result {
                    Ok(mut tuple) => {
                        if tuple.len() <= l {
                            return Some(Err(Error::Value("left index out of bound".to_string())));
                        }

                        let joined_field = tuple[l].clone();
                        match inner_table.get(&joined_field) {
                            Some(hit) => {
                                //TODO: consider late vs early materialization
                                tuple.extend(hit.clone());
                                return Some(Ok(tuple));
                            }
                            None => return None,
                        };
                    }
                    Err(err) => Some(Err(err)),
                });
                return Ok(ResultSet::Query {
                    columns: outer_columns,
                    rows: Box::new(joined_rows),
                });
            }
        };
        Err(Error::Internal("unimplemented".into()))
    }
}

impl<T: Transaction> HashInnerJoin<T> {
    pub fn new(
        left: Box<dyn Executor<T>>,
        left_field: usize,
        right: Box<dyn Executor<T>>,
        right_field: usize,
        outer: bool,
    ) -> Box<Self> {
        if outer {
            panic!("unimplemented");
        }
        Box::new(HashInnerJoin {
            left,
            left_field,
            right,
            right_field,
            outer,
        })
    }
}
