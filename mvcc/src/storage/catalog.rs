use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};

use crate::types::Oid;

use super::tile::TileGroup;

static OID: AtomicU32 = AtomicU32::new(0);
static LOCATOR: Vec<AtomicPtr<TileGroup>> = vec![];

pub fn next_oid() -> Oid {
    OID.fetch_add(1, Ordering::Relaxed)
}

pub fn get_tile_group<'a>(oid: usize) -> &'a TileGroup {
    if oid > LOCATOR.len() {
        panic!("exceed max tilegroup count")
    }
    let mut debug_count = 0;
    loop {
        let a = LOCATOR[oid].load(Ordering::Relaxed);
        if !a.is_null() {
            return &unsafe { *a };
        }
        debug_count += 1;
        if debug_count == 10 {
            panic!("something wrong");
        }
    }
}
