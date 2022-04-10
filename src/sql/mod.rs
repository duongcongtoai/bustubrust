use self::exe::ExecutionContext;
pub use self::{
    column::ColumnRef,
    schema::{ColumnInfo, DataType, Schema, TableMeta},
};
use arrow::error::ArrowError;
pub use arrow::record_batch::RecordBatch;
use core::fmt::Debug;
use serde_derive::{Deserialize, Serialize};

mod column;
pub mod exe;
pub mod insert;
mod join;
pub mod scan;
pub mod schema;
pub mod table_gen;
pub mod tx;
pub mod util;
// pub(crate) use self::column::hash_array_primitive;

pub type DataBlock = RecordBatch;

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

// impl From<StrErr> for Error {
//     fn from(s: StrErr) -> Error {
//         Error::Value(s.root)
//     }
// }
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
