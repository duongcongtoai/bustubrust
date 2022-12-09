use std::{
    borrow::Borrow,
    cell::RefCell,
    collections::HashMap,
    rc::Rc,
    sync::atomic::{AtomicU32, Ordering},
};

use dashmap::DashMap;

use crate::types::{ItemPointer, Oid, INVALID_OID};

use super::{catalog, manager::StorageManager, tile::TileGroup, tuple::Tuple};

pub type SharedTG = Rc<RefCell<TileGroup>>;
pub type ColumnMap = HashMap<Oid, (Oid, Oid)>;

pub struct DataTable {
    storage_manager: StorageManager,
    id: Oid,
    name: String,
    schema: Schema,
    tuples_per_tilegroup: usize,
    last_tile_group: AtomicU32,
    tile_groups: DashMap<Oid, SharedTG>,
    column_map: ColumnMap,
}

impl DataTable {
    pub fn tile_group_iter(&self) {
        for (_, item) in self.tile_groups {
            let tilegroup = item;
        }
    }
    pub fn get_schema(&self) -> Schema {
        self.schema.clone()
    }
    pub fn insert_tuple(&self, tuple: Tuple) -> ItemPointer {
        self.fill_in_empty_tuple_slot(tuple)
        // TODO: insert index, check fk ...
    }

    pub fn fill_in_empty_tuple_slot(&self, tuple: Tuple) -> ItemPointer {
        // call gc if there is an available recycled tuple slot -> minor, impl later
        let mut debug_count = 0;

        loop {
            let tile_group_id = self.last_tile_group.load(Ordering::Relaxed);
            let tile_group = self.tile_groups.get(&tile_group_id).unwrap();
            let tuple_slot = tile_group.borrow_mut().insert_tuple(&tuple);
            if tuple_slot != INVALID_OID {
                if tuple_slot as usize == tile_group.borrow().get_allocated_tuple_count() - 1 {
                    self.add_default_tile_group();
                }
                return ItemPointer::new(tile_group_id, tuple_slot);
            }
            debug_count += 1;
            if debug_count == 10 {
                panic!("something wrong");
            }
        }
    }

    /// allocate a new tilegroup  
    /// remmeber to update last_tile_group, vector of tilegroup
    pub fn add_default_tile_group(&self) -> Oid {
        let tile_group = self.create_tilegroup_from_column_layout();
        let tile_group_id = tile_group.borrow().get_tile_group_id();
        // TODO: is Relaxed safe here?
        self.last_tile_group.store(tile_group_id, Ordering::Relaxed);
        self.tile_groups.insert(tile_group_id, tile_group);
        let raw_tilegroup = unsafe { (*tile_group).as_ptr() };
        // TODO: this is not thread safe =))
        catalog::set_tile_group(tile_group_id, raw_tilegroup);

        return tile_group_id;
    }

    /// originally have ROW, COLUMN and HYBRID
    /// only support column layout now
    /// each sub schema is only has 1 column
    pub fn create_tilegroup_from_column_layout(&self) -> Rc<RefCell<TileGroup>> {
        let mut schemas = vec![];

        for i in 0..self.schema.get_column_count() as Oid {
            let col = self.schema.get_column(i);
            schemas.push(Schema::new(vec![col]));
        }

        TileGroup::new(
            catalog::next_oid(),
            &self.storage_manager,
            schemas,
            HashMap::new(),
            self.tuples_per_tilegroup,
        )
    }
    pub fn new<T>(
        schema: &Schema,
        storage_manager: StorageManager,
        name: T,
        db_id: Oid,
        table_id: Oid,
        tuples_per_tilegroup: usize,
    ) -> Self
    where
        T: Into<String>,
    {
        let mut default_colmap = HashMap::new();
        let col_count = schema.get_column_count();
        for i in 0..col_count as Oid {
            default_colmap.insert(i, (i, 0)).unwrap();
        }
        let table = DataTable {
            storage_manager,
            tile_groups: DashMap::new(),
            last_tile_group: AtomicU32::new(INVALID_OID),
            tuples_per_tilegroup,
            id: table_id,
            name: name.into(),
            schema: schema.clone(),
            column_map: default_colmap,
        };
        table.add_default_tile_group();
        table
    }
}

pub mod test_util {

    use rand::{thread_rng, Rng};

    use crate::{
        storage::tuple::{Tuple, Value},
        types::{Oid, INVALID_OID},
    };

    /// helloo
    use super::{Column, DataTable, ValueType};

    pub fn create_table() -> DataTable {
        DataTable::new()
    }

    pub fn populate_table(table: &DataTable, num_rows: usize, is_random: bool) {
        let schema = table.schema.clone();
        for row_id in 0..num_rows as Oid {
            let mut populated_tuple_id = row_id;

            let tuple = Tuple::new(&schema);

            let mut col_1_populated_tuple_id = populated_tuple_id;
            let mut col_2_populated_tuple_id = populated_tuple_id;
            if is_random {
                // to ensure this column always have duplicate value
                col_1_populated_tuple_id = thread_rng().gen_range(0..(num_rows / 3) as Oid);
                col_2_populated_tuple_id = thread_rng().gen();
            }
            tuple.set_value(
                0,
                Value::new_integer(get_populated_value(populated_tuple_id, 0)),
            );
            tuple.set_value(
                1,
                Value::new_integer(get_populated_value(col_1_populated_tuple_id, 1)),
            );
            tuple.set_value(
                2,
                Value::new_double(get_populated_value(col_2_populated_tuple_id, 2) as f64),
            );
            let location = table.insert_tuple(tuple);
            assert_ne!(location.block, INVALID_OID);

            //todo: tx manager.perform_insert
        }
    }

    pub fn gen_col(index: usize) -> Column {
        match index {
            0 => Column::new_static(ValueType::Integer, "col_a"),
            1 => Column::new_static(ValueType::Integer, "col_b"),
            2 => Column::new_static(ValueType::Double, "col_c"),
            3 => Column::new_dynamic(ValueType::Varchar, "col_d", 25),
            _ => {
                panic!("unknown column {}", index);
            }
        }
    }

    /// I don't know why we should do this, maybe this is a indirection layer, given the location
    /// we know its value
    pub fn get_populated_value(tuple_id: Oid, column_id: Oid) -> i32 {
        return 10 * tuple_id as i32 + column_id as i32;
    }
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
    pub fn get_column_count(&self) -> usize {
        self.cols.len()
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
    pub fn get_column(&self, col_id: Oid) -> Column {
        self.cols[col_id as usize].clone()
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
