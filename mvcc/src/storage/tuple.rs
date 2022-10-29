use crate::{storage::table::ValueType, types::Oid};
use byteorder::{BigEndian, ByteOrder};

use super::table::Schema;

pub struct Tuple {
    data: Vec<u8>,
    schema: Schema,
}
pub struct BorrowedTuple<'a> {
    data: &'a mut [u8],
    schema: Schema,
}
impl<'a> BorrowedTuple<'a> {
    pub fn new(schema: &Schema, data: &'a mut [u8]) -> Self {
        // todo: peloton round this number to be divisible by 4 somehow
        BorrowedTuple {
            data,
            schema: schema.clone(),
        }
    }

    /// TODO: support varlen type
    pub fn set_value(&mut self, col_id: Oid, val: Value) {
        let value_type = self.schema.get_type(col_id);
        let value_location = self.get_data_ref_mut(col_id);
        val.save_to_location(value_location, value_type.get_length());
    }
    pub fn get_value(&self, col_id: Oid) -> Value {
        let value_type = self.schema.get_type(col_id);
        let value_location = self.get_data_ref(col_id);
        Value::new_from_location(value_location, value_type)
    }

    fn get_data_ref(&self, col_id: Oid) -> &[u8] {
        let offset = self.schema.get_col_offset(col_id);
        &self.data[offset..]
    }

    fn get_data_ref_mut(&mut self, col_id: Oid) -> &mut [u8] {
        let offset = self.schema.get_col_offset(col_id);
        &mut self.data[offset..]
    }
}

impl Tuple {
    pub fn new(schema: &Schema) -> Self {
        // todo: peloton round this number to be divisible by 4 somehow
        let length = schema.get_length();
        Tuple {
            data: vec![0; length],
            schema: schema.clone(),
        }
    }

    /// TODO: support varlen type
    pub fn set_value(&mut self, col_id: Oid, val: Value) {
        let value_type = self.schema.get_type(col_id);
        let value_location = self.get_data_ref_mut(col_id);
        val.save_to_location(value_location, value_type.get_length());
    }
    pub fn get_value(&self, col_id: Oid) -> Value {
        let value_type = self.schema.get_type(col_id);
        let value_location = self.get_data_ref(col_id);
        Value::new_from_location(value_location, value_type)
    }

    fn get_data_ref(&self, col_id: Oid) -> &[u8] {
        let offset = self.schema.get_col_offset(col_id);
        &self.data[offset..]
    }

    fn get_data_ref_mut(&mut self, col_id: Oid) -> &mut [u8] {
        let offset = self.schema.get_col_offset(col_id);
        &mut self.data[offset..]
    }
}

pub struct Value {
    raw: [u8; 16],
    value_type: ValueType,
}

impl Value {
    pub fn new_from_location(storage: &[u8], value_type: ValueType) -> Self {
        let mut raw = [0; 16];
        let length = value_type.get_length();
        raw[..length].clone_from_slice(&storage[..length]);
        Value { raw, value_type }
    }
    pub fn new_integer(val: i32) -> Self {
        let mut raw = [0; 16];
        BigEndian::write_i32(&mut raw, val);
        Value {
            raw,
            value_type: ValueType::Integer,
        }
    }
    pub fn new_double(val: f64) -> Self {
        let mut raw = [0; 16];
        BigEndian::write_f64(&mut raw, val);
        Value {
            raw,
            value_type: ValueType::Double,
        }
    }

    pub fn save_to_location(&self, storage: &mut [u8], max_length: usize) {
        let length = self.value_type.get_length();
        storage[..length].clone_from_slice(&self.raw[..length]);
    }

    pub fn get_integer(&self) -> i32 {
        assert!(matches!(self.value_type, ValueType::Integer));
        BigEndian::read_i32(&self.raw)
    }

    pub fn get_double(&self) -> f64 {
        assert!(matches!(self.value_type, ValueType::Double));
        BigEndian::read_f64(&self.raw)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::table::{Column, Schema, ValueType};

    use super::{Tuple, Value};

    #[test]
    fn test_set_value() {
        let schema = Schema::new(vec![
            Column::new_static(ValueType::Integer, "a"),
            Column::new_static(ValueType::Double, "b"),
        ]);
        let mut tuple = Tuple::new(&schema);
        tuple.set_value(0, Value::new_integer(1));
        tuple.set_value(1, Value::new_double(0.1));
        assert_eq!(1, tuple.get_value(0).get_integer());
        assert_eq!(0.1, tuple.get_value(1).get_double());
    }
}
