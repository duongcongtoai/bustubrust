pub use arrow::datatypes::Schema;
use serde_derive::{Deserialize, Serialize};

/* #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct Schema {
    pub columns: Vec<ColumnInfo>,
}

impl Schema {
    pub fn get_type(&self, col_name: impl ToString) -> DataType {
        for item in &self.columns {
            if item.name == col_name.to_string() {
                return item.type_id;
            }
        }
        panic!("not reached")
    }
} */

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct TableMeta {
    pub schema: Schema,
    pub name: String,
    pub oid: u32,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct ColumnInfo {
    pub index: usize,
    pub name: String,
    #[serde(default)]
    fixed_length: usize,
    #[serde(default)]
    variable_length: usize,
    pub type_id: DataType,
}
impl ColumnInfo {
    pub fn new(index: usize, name: impl ToString, type_id: DataType) -> Self {
        let fixed_length: usize;
        match type_id {
            DataType::BOOL | DataType::TINYINT => fixed_length = 1,
            DataType::INTEGER => fixed_length = 4,
            _ => panic!("unimplmeneted"),
        }
        ColumnInfo {
            index,
            name: name.to_string(),
            fixed_length,
            type_id,
            variable_length: 0,
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug)]
pub enum DataType {
    INVALID,
    BOOL,
    TINYINT,
    SMALLINT,
    INTEGER,
    BIGINT,
    DECIMAL,
    VARCHAR,
    TIMESTAMP,
}
