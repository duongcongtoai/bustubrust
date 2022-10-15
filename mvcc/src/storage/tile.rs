use std::{collections::HashMap, rc::Rc};

pub struct TileGroup {
    tile: Vec<Tile>,
    schemas: Vec<Schema>,
    col_map: HashMap<usize, (usize, usize)>,
}

impl TileGroup {
    fn new(
        schemas: Vec<Schema>,
        col_map: HashMap<usize, (usize, usize)>,
        tuple_count: usize,
    ) -> Self {
        let tilegroup_header = TileGroupHeader::new(tuple_count);
        for i in 0..schemas.len() {
            let tile = Tile::new();
        }
        false;
    }
}
pub struct Tile {}

impl Tile {
    fn new(tile_header: Rc<TileGroupHeader>, schema: Schema, tuple_count: usize) -> Self {
        let tile_size = tuple_count * schema.tuple_length;
    }
}
pub struct TileGroupHeader {}

impl TileGroupHeader {
    fn new(tuple_count: usize) -> Self {}
}

pub struct Schema {
    cols: Vec<Column>,
    col_types: Vec<ValueType>,
    col_names: Vec<String>,
    col_lengths: Vec<usize>,
    col_is_inlined: Vec<bool>,
    tuple_length: usize,
}
impl Schema {
    fn new(cols: Vec<Column>) -> Self {
        let (mut col_types, mut col_names, mut col_lengths, mut col_is_inlined) =
            (vec![], vec![], vec![], vec![]);
        for col in cols.iter() {
            col_types.push(col.value_type);
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
}
pub struct Column {
    value_type: ValueType,
    length: usize,
    name: String,
    is_inlined: bool,
    col_offset: usize,
}
impl Column {
    fn new_dynamic<I: Into<String>>(value_type: ValueType, name: I, length: usize) -> Self {
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
    fn new_static<I: Into<String>>(value_type: ValueType, name: I) -> Self {
        let mut length = 0;
        match value_type {
            ValueType::Integer => length = 4,
            ValueType::TinyInt => length = 1,
            _ => {
                panic!("type can't be static {:?}", value_type)
            }
        };
        Column {
            value_type,
            name: name.into(),
            length,
            is_inlined: true,
            col_offset: 0,
        }
    }
}
#[derive(Debug)]
pub enum ValueType {
    Integer,
    TinyInt,
    Varchar,
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
