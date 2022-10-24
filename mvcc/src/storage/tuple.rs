use super::tile::{Schema, ValueType};
use crate::types::Oid;
use byteorder::{BigEndian, ByteOrder};

pub struct Tuple {
    data: Vec<u8>,
    schema: Schema,
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
    pub fn set_value(&self, col_id: Oid, val: Value) {
        let length = self.schema.get_length();
        let value_type = self.schema.get_type(col_id);
        let value_location = self.get_data_ref(col_id);
        val.save_to_location(value_location, length);
    }

    fn get_data_ref(&mut self, col_id: Oid) -> &mut [u8] {
        let offset = self.schema.get_col_offset(col_id);
        &mut self.data[offset..]
    }
}

pub struct Value {
    raw: [u8; 16],
    value_type: ValueType,
}

impl Value {
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
}
