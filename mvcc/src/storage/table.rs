use crate::types::Oid;

use super::{storage::ContainerTuple, tuple::Tuple};

pub struct DataTable {
    id: Oid,
    name: String,
    schema: Schema,
    tuples_per_tilegroup: usize,
    last_tile_group: Oid,
}

pub fn populate_table(table: &DataTable, num_row: usize) {
    // something to do with var len pool
    for row in 0..num_row {}

    // location = table->InsertTuple(tuple.get(), &itemptr_ptr);
    // res = transaction_manager.PerformInsert(location);
    // lo
}

impl DataTable {
    pub fn fill_in_empty_tuple_slot(tuple: Tuple) {
        // call gc if there is an available recycled tuple slot -> minor, impl later
        let mut try_count = 0;
        loop {}
    }
}

pub struct ItemPointer {
    block: Oid,
    offset: Oid,
}

#[derive(Clone)]
pub struct Schema {
    pub cols: Vec<Column>,
    col_types: Vec<ValueType>,
    col_names: Vec<String>,
    col_lengths: Vec<usize>,
    col_is_inlined: Vec<bool>,
    pub tuple_length: usize,
}
impl Schema {
    pub fn new(mut cols: Vec<Column>) -> Self {
        let (mut col_types, mut col_names, mut col_lengths, mut col_is_inlined) =
            (vec![], vec![], vec![], vec![]);
        for col in cols.iter() {
            col_types.push(col.value_type.clone());
            col_names.push(col.name.clone());
            col_lengths.push(col.length);
            col_is_inlined.push(col.is_inlined);
        }
        let mut col_offset = 0;
        for col in cols.iter_mut() {
            col.col_offset = col_offset;
            col_offset += col.length;
        }
        let tuple_length = col_offset;

        Schema {
            cols,
            col_types,
            col_names,
            col_is_inlined,
            col_lengths,
            tuple_length,
        }
    }
    pub fn get_length(&self) -> usize {
        self.tuple_length
    }
    pub fn get_type(&self, col_id: Oid) -> ValueType {
        self.col_types[col_id as usize]
    }
    pub fn get_col_offset(&self, col_id: Oid) -> usize {
        self.cols[col_id as usize].col_offset
    }
}
#[derive(Clone)]
pub struct Column {
    value_type: ValueType,
    length: usize,
    name: String,
    is_inlined: bool,
    col_offset: usize, // within a tuple, which byte to access this column value
}
impl Column {
    pub fn new_dynamic<I: Into<String>>(value_type: ValueType, name: I, length: usize) -> Self {
        if let ValueType::Varchar = value_type {
            return Column {
                value_type,
                name: name.into(),
                length,
                is_inlined: false,
                col_offset: 0,
            };
        }
        panic!("invalid value type")
    }
    pub fn new_static<I: Into<String>>(value_type: ValueType, name: I) -> Self {
        let mut length = 0;
        let length = value_type.get_length();
        Column {
            value_type,
            name: name.into(),
            length,
            is_inlined: true,
            col_offset: 0,
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub enum ValueType {
    Integer,
    Double,
    TinyInt,
    Varchar,
}
impl ValueType {
    pub fn get_length(&self) -> usize {
        match self {
            ValueType::Integer => 4,
            ValueType::TinyInt => 1,
            ValueType::Double => 8,
            _ => {
                panic!("cannot get length of type {:?}", self)
            }
        }
    }
}
