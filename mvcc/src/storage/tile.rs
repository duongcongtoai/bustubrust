use crate::types::Oid;
use libc::c_void;
use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::Arc};

use super::{
    manager::StorageManager,
    storage::{ContainerTuple, Value},
    tuple::Tuple,
};

pub struct TileGroup {
    tiles: Vec<Tile>,
    schemas: Vec<Schema>,
    col_map: HashMap<usize, (usize, usize)>,
}

impl TileGroup {
    fn new(
        storage: &StorageManager,
        schemas: Vec<Schema>,
        col_map: HashMap<usize, (usize, usize)>,
        tuple_count: usize,
    ) -> Rc<RefCell<Self>> {
        let tilegroup_header = TileGroupHeader::new(storage, tuple_count);
        let shared_header = Rc::new(RefCell::new(tilegroup_header));
        let tile_group = TileGroup {
            tiles: vec![],
            schemas,
            col_map,
        };
        let shared_tg = Rc::new(RefCell::new(tile_group));
        for i in 0..shared_tg.borrow().schemas.len() {
            let tile = Tile::new(
                storage,
                shared_header.clone(),
                shared_tg.clone(),
                &shared_tg.borrow().schemas[i],
                tuple_count,
            );
            shared_tg.borrow_mut().tiles.push(tile);
        }
        shared_tg
    }
}
pub struct Tile {
    data: *mut c_void,
    tile_group: Rc<RefCell<TileGroup>>,
    tile_group_header: Rc<RefCell<TileGroupHeader>>,
    schema: Schema,
}

impl Tile {
    fn new(
        storage: &StorageManager,
        tile_group_header: Rc<RefCell<TileGroupHeader>>,
        tile_group: Rc<RefCell<TileGroup>>,
        schema: &Schema,
        tuple_count: usize,
    ) -> Self {
        let tile_size = tuple_count * schema.tuple_length;
        let data = storage.allocate(tile_size);
        Tile {
            data,
            tile_group,
            tile_group_header,
            schema: schema.clone(),
        }
    }
}
pub struct TileGroupHeader {}

impl TileGroupHeader {
    fn new(storage: &StorageManager, tuple_count: usize) -> Self {
        unimplemented!()
    }
}
pub struct LogicalTile {
    position_lists: Vec<Vec<Oid>>,
}
impl LogicalTile {
    fn new() -> Self {
        LogicalTile {
            position_lists: vec![],
        }
    }
}

#[derive(Clone)]
pub struct Schema {
    cols: Vec<Column>,
    col_types: Vec<ValueType>,
    col_names: Vec<String>,
    col_lengths: Vec<usize>,
    col_is_inlined: Vec<bool>,
    tuple_length: usize,
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

#[cfg(test)]
mod tests {
    use super::{
        Column, Schema,
        ValueType::{Integer, TinyInt, Varchar},
    };

    #[test]
    fn test_tile() {
        let col1 = Column::new_static(Integer, "A");
        let col2 = Column::new_static(Integer, "B");
        let col3 = Column::new_static(TinyInt, "C");
        let col4 = Column::new_dynamic(Varchar, "D", 50);
        let schema1 = Schema::new(vec![col1, col2]);
        let schema2 = Schema::new(vec![col3, col4]);
        let schemas = vec![schema1, schema2];
    }
}
pub fn populate_tile(schema: Schema, tile_group: &TileGroup, num_rows: usize) {
    for tuple_id in 0..num_rows {
        let tuple = Tuple::new(&schema);
    }
}
