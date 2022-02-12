mod executor;
mod join;
mod plan;
mod tx;

use crate::error::Result;
use crate::sql::executor::Value;
use crate::sql::plan::Node;
use derivative::Derivative;
use serde_derive::{Deserialize, Serialize};

#[derive(Derivative, Serialize, Deserialize)]
#[derivative(Debug, PartialEq)]
pub enum ResultSet {
    // Transaction started
    /* Begin {
        id: u64,
        mode: Mode,
    },
    // Transaction committed
    Commit {
        id: u64,
    },
    // Transaction rolled back
    Rollback {
        id: u64,
    },
    // Rows created
    Create {
        count: u64,
    },
    // Rows deleted
    Delete {
        count: u64,
    },
    // Rows updated
    Update {
        count: u64,
    },
    // Table created
    CreateTable {
        name: String,
    },
    // Table dropped
    DropTable {
        name: String,
    }, */
    // Query result
    Query {
        columns: Columns,
        #[derivative(Debug = "ignore")]
        #[derivative(PartialEq = "ignore")]
        #[serde(skip, default = "ResultSet::empty_rows")]
        rows: Rows,
    },
    // Explain result
    Explain(Node),
}

/// A row of values
pub type Row = Vec<Value>;

/// A row iterator
pub type Rows = Box<dyn Iterator<Item = Result<Row>> + Send>;

/// A column (in a result set, see schema::Column for table columns)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: Option<String>,
}

/// A set of columns
pub type Columns = Vec<Column>;

impl ResultSet {
    /// Creates an empty row iterator, for use by serde(default).
    fn empty_rows() -> Rows {
        Box::new(std::iter::empty())
    }
}

pub struct Nothing;

impl Nothing {
    pub fn new() -> Box<Self> {
        Box::new(Self)
    }
}
