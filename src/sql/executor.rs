use super::plan::Node;
use crate::error::{Error, Result};
use crate::sql::join::HashInnerJoin;
use crate::sql::tx::Transaction;
use crate::sql::Nothing;
use crate::sql::ResultSet;
use crate::sql::Row;
use serde_derive::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

pub trait Executor<T: Transaction> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet>;
}

impl<T: Transaction + 'static> dyn Executor<T> {
    /// Builds an executor for a plan node, consuming it
    pub fn build(node: Node) -> Box<dyn Executor<T>> {
        match node {
            Node::HashJoin {
                left,
                left_field,
                right,
                right_field,
                outer,
            } => HashInnerJoin::new(
                Self::build(*left),
                left_field.0,
                Self::build(*right),
                right_field.0,
                outer,
            ),
            Node::Nothing => Nothing::new(),
        }
    }
}

/* pub struct Tuple<'a> {
    data: &'a [u8],
}
pub struct Schema {}

pub struct Rid {}

impl<'a> Tuple<'a> {
    fn get_rid() -> Rid {
        Rid {}
    }

    fn get_data(&self) -> &'a [u8] {
        return self.data;
    }
} */

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl std::cmp::Eq for Value {}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.datatype().hash(state);
        match self {
            Value::Null => self.hash(state),
            Value::Boolean(v) => v.hash(state),
            Value::Integer(v) => v.hash(state),
            Value::Float(v) => v.to_be_bytes().hash(state),
            Value::String(v) => v.hash(state),
        }
    }
}

impl Value {
    /// Returns the value's datatype, or None for null values
    pub fn datatype(&self) -> Option<DataType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(DataType::Boolean),
            Self::Integer(_) => Some(DataType::Integer),
            Self::Float(_) => Some(DataType::Float),
            Self::String(_) => Some(DataType::String),
        }
    }

    /// Returns the inner boolean, or an error if not a boolean
    pub fn boolean(self) -> Result<bool> {
        match self {
            Self::Boolean(b) => Ok(b),
            v => Err(Error::Value(format!("Not a boolean: {:?}", v))),
        }
    }

    /// Returns the inner float, or an error if not a float
    pub fn float(self) -> Result<f64> {
        match self {
            Self::Float(f) => Ok(f),
            v => Err(Error::Value(format!("Not a float: {:?}", v))),
        }
    }

    /// Returns the inner integer, or an error if not an integer
    pub fn integer(self) -> Result<i64> {
        match self {
            Self::Integer(i) => Ok(i),
            v => Err(Error::Value(format!("Not an integer: {:?}", v))),
        }
    }

    /// Returns the inner string, or an error if not a string
    pub fn string(self) -> Result<String> {
        match self {
            Self::String(s) => Ok(s),
            v => Err(Error::Value(format!("Not a string: {:?}", v))),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(
            match self {
                Self::Null => "NULL".to_string(),
                Self::Boolean(b) if *b => "TRUE".to_string(),
                Self::Boolean(_) => "FALSE".to_string(),
                Self::Integer(i) => i.to_string(),
                Self::Float(f) => f.to_string(),
                Self::String(s) => s.clone(),
            }
            .as_ref(),
        )
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

impl<T: Transaction> Executor<T> for Nothing {
    fn execute(self: Box<Self>, _: &mut T) -> Result<ResultSet> {
        Ok(ResultSet::Query {
            columns: Vec::new(),
            rows: Box::new(std::iter::once(Ok(Row::new()))),
        })
    }
}
