use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};

use crate::types::Oid;

use super::tile::TileGroup;

static OID: AtomicU32 = AtomicU32::new(0);

static LOCATOR: Vec<AtomicPtr<TileGroup>> = vec![];

pub fn next_oid() -> Oid {
    OID.fetch_add(1, Ordering::Relaxed)
}

pub fn get_tile_group<'a>(oid: Oid) -> &'a TileGroup {
    loop {
        let ptr = LOCATOR[oid as usize].load(Ordering::Relaxed);
        if !ptr.is_null() {
            return &unsafe { *ptr };
        }
    }
}
