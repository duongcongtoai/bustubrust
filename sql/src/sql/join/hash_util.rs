// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Functionality used both on logical and physical plans

use crate::sql::SqlResult;
use ahash::{CallHasher, RandomState};
use datafusion::arrow::{
    array::{
        Array, ArrayRef, BooleanArray, DecimalArray, DictionaryArray, Int16Array, Int32Array,
        Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{ArrowDictionaryKeyType, ArrowNativeType, DataType},
};
use std::sync::Arc;

// Combines two hashes into one hash
#[inline]
fn combine_hashes(l: u64, r: u64) -> u64 {
    let hash = (17 * 37u64).wrapping_add(l);
    hash.wrapping_mul(37).wrapping_add(r)
}

fn hash_decimal128<'a>(
    array: &ArrayRef,
    random_state: &RandomState,
    hashes_buffer: &'a mut Vec<u64>,
    mul_col: bool,
) {
    let array = array.as_any().downcast_ref::<DecimalArray>().unwrap();
    if array.null_count() == 0 {
        if mul_col {
            for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                *hash = combine_hashes(i128::get_hash(&array.value(i), random_state), *hash);
            }
        } else {
            for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                *hash = i128::get_hash(&array.value(i), random_state);
            }
        }
    } else if mul_col {
        for (i, hash) in hashes_buffer.iter_mut().enumerate() {
            if !array.is_null(i) {
                *hash = combine_hashes(i128::get_hash(&array.value(i), random_state), *hash);
            }
        }
    } else {
        for (i, hash) in hashes_buffer.iter_mut().enumerate() {
            if !array.is_null(i) {
                *hash = i128::get_hash(&array.value(i), random_state);
            }
        }
    }
}

macro_rules! hash_array {
    ($array_type:ident, $column: ident, $ty: ident, $hashes: ident, $random_state: ident, $multi_col: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        if array.null_count() == 0 {
            if $multi_col {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    *hash = combine_hashes($ty::get_hash(&array.value(i), $random_state), *hash);
                }
            } else {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    *hash = $ty::get_hash(&array.value(i), $random_state);
                }
            }
        } else {
            if $multi_col {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    if !array.is_null(i) {
                        *hash =
                            combine_hashes($ty::get_hash(&array.value(i), $random_state), *hash);
                    }
                }
            } else {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    if !array.is_null(i) {
                        *hash = $ty::get_hash(&array.value(i), $random_state);
                    }
                }
            }
        }
    };
}

macro_rules! hash_array_primitive {
    ($array_type:ident, $column: ident, $ty: ident, $hashes: ident, $random_state: ident, $multi_col: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        let values = array.values();

        if array.null_count() == 0 {
            if $multi_col {
                for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                    *hash = combine_hashes($ty::get_hash(value, $random_state), *hash);
                }
            } else {
                for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                    *hash = $ty::get_hash(value, $random_state)
                }
            }
        } else {
            if $multi_col {
                for (i, (hash, value)) in $hashes.iter_mut().zip(values.iter()).enumerate() {
                    if !array.is_null(i) {
                        *hash = combine_hashes($ty::get_hash(value, $random_state), *hash);
                    }
                }
            } else {
                for (i, (hash, value)) in $hashes.iter_mut().zip(values.iter()).enumerate() {
                    if !array.is_null(i) {
                        *hash = $ty::get_hash(value, $random_state);
                    }
                }
            }
        }
    };
}

macro_rules! hash_array_float {
    ($array_type:ident, $column: ident, $ty: ident, $hashes: ident, $random_state: ident, $multi_col: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        let values = array.values();

        if array.null_count() == 0 {
            if $multi_col {
                for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                    *hash = combine_hashes(
                        $ty::get_hash(&$ty::from_le_bytes(value.to_le_bytes()), $random_state),
                        *hash,
                    );
                }
            } else {
                for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                    *hash = $ty::get_hash(&$ty::from_le_bytes(value.to_le_bytes()), $random_state)
                }
            }
        } else {
            if $multi_col {
                for (i, (hash, value)) in $hashes.iter_mut().zip(values.iter()).enumerate() {
                    if !array.is_null(i) {
                        *hash = combine_hashes(
                            $ty::get_hash(&$ty::from_le_bytes(value.to_le_bytes()), $random_state),
                            *hash,
                        );
                    }
                }
            } else {
                for (i, (hash, value)) in $hashes.iter_mut().zip(values.iter()).enumerate() {
                    if !array.is_null(i) {
                        *hash =
                            $ty::get_hash(&$ty::from_le_bytes(value.to_le_bytes()), $random_state);
                    }
                }
            }
        }
    };
}

/// Hash the values in a dictionary array
fn create_hashes_dictionary<K: ArrowDictionaryKeyType>(
    array: &ArrayRef,
    random_state: &RandomState,
    hashes_buffer: &mut Vec<u64>,
    multi_col: bool,
) -> SqlResult<()> {
    let dict_array = array.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();

    // Hash each dictionary value once, and then use that computed
    // hash for each key value to avoid a potentially expensive
    // redundant hashing for large dictionary elements (e.g. strings)
    let dict_values = Arc::clone(dict_array.values());
    let mut dict_hashes = vec![0; dict_values.len()];
    create_hashes(&[dict_values], random_state, &mut dict_hashes)?;

    // combine hash for each index in values
    if multi_col {
        for (hash, key) in hashes_buffer.iter_mut().zip(dict_array.keys().iter()) {
            if let Some(key) = key {
                let idx = key.to_usize().ok_or_else(|| {
                    format!(
                        "Can not convert key value {:?} to usize in dictionary of type {:?}",
                        key,
                        dict_array.data_type()
                    )
                    .to_string()
                })?;
                *hash = combine_hashes(dict_hashes[idx], *hash)
            } // no update for Null, consistent with other hashes
        }
    } else {
        for (hash, key) in hashes_buffer.iter_mut().zip(dict_array.keys().iter()) {
            if let Some(key) = key {
                let idx = key.to_usize().ok_or_else(|| {
                    format!(
                        "Can not convert key value {:?} to usize in dictionary of type {:?}",
                        key,
                        dict_array.data_type()
                    )
                })?;
                *hash = dict_hashes[idx]
            } // no update for Null, consistent with other hashes
        }
    }
    Ok(())
}

/// Test version of `create_hashes` that produces the same value for
/// all hashes (to test collisions)
///
/// See comments on `hashes_buffer` for more details
#[cfg(feature = "force_hash_collisions")]
pub fn create_hashes<'a>(
    _arrays: &[ArrayRef],
    _random_state: RandomState,
    hashes_buffer: &'a mut Vec<u64>,
) -> Result<&'a mut Vec<u64>> {
    for hash in hashes_buffer.iter_mut() {
        *hash = 0
    }
    return Ok(hashes_buffer);
}

/// Return vector of bucket index
pub fn hash_to_buckets<'a>(
    arrays: &[ArrayRef],
    random_state: &RandomState,
    hashes_buffer: &'a mut Vec<u64>,
    buckets: usize,
) -> SqlResult<()> {
    // combine hashes with `combine_hashes` if we have more than 1 column
    let multi_col = arrays.len() > 1;

    for col in arrays {
        match col.data_type() {
            DataType::Decimal(_, _) => {
                hash_decimal128(col, &random_state, hashes_buffer, multi_col);
            }
            DataType::UInt8 => {
                hash_array_primitive!(UInt8Array, col, u8, hashes_buffer, random_state, multi_col);
            }
            DataType::UInt16 => {
                hash_array_primitive!(
                    UInt16Array,
                    col,
                    u16,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::UInt32 => {
                hash_array_primitive!(
                    UInt32Array,
                    col,
                    u32,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::UInt64 => {
                hash_array_primitive!(
                    UInt64Array,
                    col,
                    u64,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Int8 => {
                hash_array_primitive!(Int8Array, col, i8, hashes_buffer, random_state, multi_col);
            }
            DataType::Int16 => {
                hash_array_primitive!(Int16Array, col, i16, hashes_buffer, random_state, multi_col);
            }
            DataType::Int32 => {
                hash_array_primitive!(Int32Array, col, i32, hashes_buffer, random_state, multi_col);
            }
            DataType::Int64 => {
                hash_array_primitive!(Int64Array, col, i64, hashes_buffer, random_state, multi_col);
            }
            DataType::Boolean => {
                hash_array!(
                    BooleanArray,
                    col,
                    u8,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }

            _ => {
                // This is internal because we should have caught this before.
                return Err(
                    format!("Unsupported data type in hasher: {}", col.data_type())
                        .to_string()
                        .into(),
                );
            }
        }
    }
    for item in hashes_buffer.iter_mut() {
        *item = *item % buckets as u64;
    }
    Ok(())
}

/// Creates hash values for every row, based on the values in the
/// columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately
#[cfg(not(feature = "force_hash_collisions"))]
pub fn create_hashes<'a>(
    arrays: &[ArrayRef],
    random_state: &RandomState,
    hashes_buffer: &'a mut Vec<u64>,
) -> SqlResult<&'a mut Vec<u64>> {
    // combine hashes with `combine_hashes` if we have more than 1 column
    let multi_col = arrays.len() > 1;

    for col in arrays {
        match col.data_type() {
            DataType::Decimal(_, _) => {
                hash_decimal128(col, &random_state, hashes_buffer, multi_col);
            }
            DataType::UInt8 => {
                hash_array_primitive!(UInt8Array, col, u8, hashes_buffer, random_state, multi_col);
            }
            DataType::UInt16 => {
                hash_array_primitive!(
                    UInt16Array,
                    col,
                    u16,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::UInt32 => {
                hash_array_primitive!(
                    UInt32Array,
                    col,
                    u32,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::UInt64 => {
                hash_array_primitive!(
                    UInt64Array,
                    col,
                    u64,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Int8 => {
                hash_array_primitive!(Int8Array, col, i8, hashes_buffer, random_state, multi_col);
            }
            DataType::Int16 => {
                hash_array_primitive!(Int16Array, col, i16, hashes_buffer, random_state, multi_col);
            }
            DataType::Int32 => {
                hash_array_primitive!(Int32Array, col, i32, hashes_buffer, random_state, multi_col);
            }
            DataType::Int64 => {
                hash_array_primitive!(Int64Array, col, i64, hashes_buffer, random_state, multi_col);
            }
            DataType::Boolean => {
                hash_array!(
                    BooleanArray,
                    col,
                    u8,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }

            _ => {
                // This is internal because we should have caught this before.
                return Err(format!("Unsupported data type in hasher: {}", col.data_type()).into());
            }
        }
    }
    Ok(hashes_buffer)
}
