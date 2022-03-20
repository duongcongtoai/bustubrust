use core::fmt::Debug;
use core::fmt::Formatter;
use serde_derive::{Deserialize, Serialize};

use crate::bpm::StrErr;

use self::exe::ExecutionContext;

pub mod exe;
mod join;
mod scan;
pub mod tx;

pub struct Batch {
    inner: Vec<Row>,
}
impl Debug for Batch {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let st = self
            .inner
            .iter()
            .map(|row| row.string_data(8))
            .collect::<Vec<String>>()
            .join(",");

        f.write_str("{")?;
        f.write_str(&st)?;
        f.write_str("}")?;
        Ok(())
    }
}
impl Debug for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(&self.string_data(8))
    }
}
impl Batch {
    pub fn new(r: Vec<Row>) -> Self {
        Batch { inner: r }
    }
    pub fn data(&self) -> &Vec<Row> {
        &self.inner
    }
}

#[derive(Clone)]
pub struct Row {
    pub inner: Vec<u8>,
}
impl Row {
    fn new(inner: Vec<u8>) -> Self {
        Row { inner }
    }
    pub fn string_data(&self, key_offset: usize) -> String {
        String::from_utf8(self.inner[key_offset..].to_vec()).unwrap()
    }
}

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

impl From<StrErr> for Error {
    fn from(s: StrErr) -> Error {
        Error::Value(s.root)
    }
}
impl From<serde_json::Error> for Error {
    fn from(s: serde_json::Error) -> Error {
        Error::Value(s.to_string())
    }
}

pub trait Operator {
    fn next(&mut self, e: ExecutionContext) -> SqlResult<Batch>;
}
