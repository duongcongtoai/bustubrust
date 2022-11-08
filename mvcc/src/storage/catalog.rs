use std::sync::atomic::{AtomicU32, Ordering};

use crate::types::Oid;

static OID: AtomicU32 = AtomicU32::new(0);

pub fn next_oid() -> Oid {
    OID.fetch_add(1, Ordering::Relaxed)
}
