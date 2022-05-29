use self::exe::ExecutionContext;
// pub use self::schema::{ColumnInfo, TableMeta};
use core::fmt::Debug;
pub use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::{datatypes::DataType as ArrowDataType, error::ArrowError};
use serde_derive::{Deserialize, Serialize};

pub mod exe;
pub mod inmem_op;
pub mod insert;
pub mod join;
pub mod scan;
// pub mod schema;
pub mod cc;
pub mod common;
pub mod table_gen;
pub mod tx;
pub mod util;

pub type DataBlock = RecordBatch;
pub type DataType = ArrowDataType;

// TODO: rename into some generic type, not just about sql
pub type SqlResult<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Error {
    Abort,
    Config(String),
    Internal(String),
    Parse(String),
    ReadOnly,
    Serialization,
    Value(String),
}

impl From<String> for Error {
    fn from(s: String) -> Error {
        Error::Value(s)
    }
}

impl From<serde_json::Error> for Error {
    fn from(s: serde_json::Error) -> Error {
        Error::Value(s.to_string())
    }
}
impl From<ArrowError> for Error {
    fn from(s: ArrowError) -> Error {
        Error::Value(s.to_string())
    }
}
