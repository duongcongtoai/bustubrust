use self::exe::ExecutionContext;
use crate::bpm::StrErr;
use core::fmt::Debug;
use core::fmt::Formatter;
use serde_derive::{Deserialize, Serialize};

pub mod exe;
pub mod insert;
mod join;
pub mod scan;
pub mod table_gen;
pub mod tx;
pub mod util;

pub struct Batch {
    inner: Vec<Row>,
}
pub struct PartialResult {
    pub done: bool,
    pub inner: Batch,
}
impl PartialResult {
    fn new_done() -> Self {
        PartialResult {
            done: true,
            inner: Batch { inner: vec![] },
        }
    }
    fn new(rows: Vec<Row>) -> Self {
        PartialResult {
            done: false,
            inner: Batch { inner: rows },
        }
    }
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
    pub fn len(&self) -> usize {
        self.inner.len()
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
