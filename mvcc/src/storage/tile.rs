use crate::{storage::tuple::BorrowedTuple, types::Oid};
use libc::c_void;
use std::{
    cell::RefCell,
    collections::HashMap,
    rc::Rc,
    sync::atomic::{AtomicU32, Ordering},
};

use super::{
    manager::StorageManager,
    table::Schema,
    tuple::{Tuple, Value},
};

/// TODO: Deprecate this implementation and use column-based storage layout instead
pub struct TileGroup {
    id: Oid,
    tiles: Vec<Tile>,
    schemas: Vec<Schema>,
    col_map: HashMap<usize, (usize, usize)>,
    header: Rc<RefCell<TileGroupHeader>>,
}

impl TileGroup {
    pub fn get_allocated_tuple_count(&self) -> usize {
        unimplemented!()
    }
    pub fn get_tile_group_id(&self) -> Oid {
        self.id
    }

    pub fn new(
        id: Oid,
        storage: &StorageManager,
        schemas: Vec<Schema>,
        col_map: HashMap<usize, (usize, usize)>,
        tuple_count: usize,
    ) -> Rc<RefCell<Self>> {
        let tilegroup_header = TileGroupHeader::new(storage, tuple_count);
        let shared_header = Rc::new(RefCell::new(tilegroup_header));
        let tile_group = TileGroup {
            id,
            tiles: vec![],
            schemas,
            col_map,
            header: shared_header.clone(),
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

    // for example col1,col2,col3,col4,col5, tile group has 2 tile, tile1 has col1,col2,col3 and
    // tile2 has col4,col5
    pub fn insert_tuple(&self, tuple: &Tuple) -> Oid {
        let tuple_slot_id = self.header.borrow().next_empty_tuple_slot();
        if tuple_slot_id == u32::MAX {
            return tuple_slot_id;
        }
        let mut col_iter = 0;
        for tile_itr in 0..self.tiles.len() {
            let schema = &self.schemas[tile_itr];
            let col_count = schema.cols.len();
            let tile = &self.tiles[tile_itr];
            let tile_tuple_location = tile.get_tuple_location(tuple_slot_id);
            let mut tile_tuple = BorrowedTuple::new(schema, tile_tuple_location);
            for tile_column_iter in 0..col_count as Oid {
                tile_tuple.set_value(tile_column_iter, tuple.get_value(col_iter));
                col_iter += 1;
            }
        }
        return tuple_slot_id;
    }
}
pub struct Tile {
    data: *mut c_void,
    tile_group: Rc<RefCell<TileGroup>>,
    tile_group_header: Rc<RefCell<TileGroupHeader>>,
    tile_size: usize,
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
            tile_size,
            tile_group,
            tile_group_header,
            schema: schema.clone(),
        }
    }

    fn get_tuple_location(&self, tuple_slot_id: Oid) -> &mut [u8] {
        let mutptr = self.data as *mut u8;
        unsafe {
            let st = mutptr.add(tuple_slot_id as usize * self.schema.tuple_length as usize);
            return std::slice::from_raw_parts_mut(st as *mut u8, self.tile_size);
        }
    }
}
pub struct TileGroupHeader {
    next_tuple_slot: AtomicU32,
    num_tuple_slot: usize,
}

impl TileGroupHeader {
    fn new(storage: &StorageManager, tuple_count: usize) -> Self {
        TileGroupHeader {
            num_tuple_slot: tuple_count,
            next_tuple_slot: AtomicU32::new(0),
        }
    }

    pub fn next_empty_tuple_slot(&self) -> Oid {
        let tuple_slot_id = self.next_tuple_slot.fetch_add(1, Ordering::Relaxed);
        if tuple_slot_id >= self.num_tuple_slot as u32 {
            return u32::MAX;
        }
        return tuple_slot_id;
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

#[cfg(test)]
mod tests {
    /* use super::{
        Column, Schema, TileGroup,
        ValueType::{Integer, TinyInt, Varchar},
    };
    use crate::{
        exe::test_util::populated_value,
        storage::tuple::{Tuple, Value},
        types::Oid,
    };

    #[test]
    fn test_tile() {
        let col1 = Column::new_static(Integer, "A");
        let col2 = Column::new_static(Integer, "B");
        let col3 = Column::new_static(TinyInt, "C");
        // let col4 = Column::new_dynamic(Varchar, "D", 50);
        let schema1 = Schema::new(vec![col1, col2]);
        let schema2 = Schema::new(vec![col3]);
        let schemas = vec![schema1, schema2];
    } */
}
