use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct Schema {
    pub columns: Vec<Column>,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct TableMeta {
    pub schema: Schema,
    pub name: String,
    pub oid: u32,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct Column {
    pub name: String,
    #[serde(default)]
    fixed_length: usize,
    #[serde(default)]
    variable_length: usize,
    pub type_id: DataType,
}
impl Column {
    pub fn new(name: String, type_id: DataType) -> Self {
        let fixed_length: usize;
        match type_id {
            DataType::BOOL | DataType::TINYINT => fixed_length = 1,
            DataType::INTEGER => fixed_length = 4,
            _ => panic!("unimplmeneted"),
        }
        Column {
            name,
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
