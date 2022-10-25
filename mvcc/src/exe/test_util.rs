use crate::{
    storage::{
        table::DataTable,
        tile::{Column, Schema, ValueType},
    },
    types::Oid,
};

pub fn create_table(tuple_per_tile_group: usize, table_id: Oid) -> DataTable {
    unimplemented!()
    // let schema = Schema::new(vec![gen_col(1)]);
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

pub fn populated_value(tuple_id: Oid, column_id: Oid) -> i32 {
    return 10 * tuple_id as i32 + column_id as i32;
}
