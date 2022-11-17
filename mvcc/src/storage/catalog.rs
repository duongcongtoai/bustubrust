use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};

use crate::types::Oid;

use super::tile::TileGroup;

static OID: AtomicU32 = AtomicU32::new(0);
static LOCATOR: Vec<AtomicPtr<TileGroup>> = vec![];

pub fn next_oid() -> Oid {
    OID.fetch_add(1, Ordering::Relaxed)
}
pub fn set_tile_group(oid: Oid, tile_group: *mut TileGroup) {
    LOCATOR[oid as usize].store(tile_group, Ordering::Relaxed);
}

// catalog::Manager::GetInstance().AddTileGroup(tile_group_id, tile_group);

pub fn get_tile_group<'a>(oid: Oid) -> &'a TileGroup {
    if oid > LOCATOR.len() as Oid {
        panic!("exceed max tilegroup count")
    }
    let mut debug_count = 0;
    loop {
        let a = LOCATOR[oid as usize].load(Ordering::Relaxed);
        if !a.is_null() {
            return &unsafe { *a };
        }
        debug_count += 1;
        if debug_count == 10 {
            panic!("something wrong");
        }
    }
}
