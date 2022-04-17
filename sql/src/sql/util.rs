use super::exe::Operator;
use crate::sql::DataBlock;
// use crate::sql::ExecutionContext;
use crate::sql::Error;

/// Util struct to create impl of Operator from raw input
pub struct RawInput {
    inner: DataBlock,
}
