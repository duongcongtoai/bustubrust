use std::{cell::RefCell, collections::HashMap};

use crossbeam_channel::Receiver;

// a variation of MV2PL, 2V2PL
pub struct TVTPL {}

/// |   |r  |w  |c  |
/// -----------------
/// |r  |+  |+  |-  |
/// -----------------
/// |w  |+  |-  |-  |
/// -----------------
/// |c  |-  |-  |-  |
/// Lock is based on the recordID if the item, not based on the version
pub enum Lock2P2PL {
    Read,
    Write,
    Certify,
}

// serialization snapshot isolation
pub struct SSI {}

// serial safety net
pub struct SSN {}
// serialization graph tester protocol
pub struct MVSGT {}

pub struct MVOCC {}

pub struct MVTO {}

#[derive(Debug, Copy, Clone)]
pub enum DepCode {
    Abort,
    Success,
}

pub type TxID = u64;
#[derive(Debug)]
pub enum TxPhase {
    Processing,
    Preparing,
    Comitted,
    Aborted,
}

pub struct Tx {
    pub begin_ts: u32,
    state: TxPhase,
    pub id: TxID,
    total_dependencies: u64,
    // where i get my dependencies' result
    dep_result_receiver: Receiver<(TxID, DepCode)>,
    // where i get my dependent
    dep_registrations: Receiver<TxID>,
    rw_sets: RefCell<HashMap<Oid, HashMap<Oid, RWType>>>,
}
#[derive(Debug)]
pub enum RWType {
    Read,
    Update,
    Insert,
    Delete,
    InsDelete, // delete after insert
}

impl Tx {
    pub fn record_read(&self, location: ItemPointer) {
        let tg_id = location.block;
        let tuple_id = location.offset;
        if let Some(tg_rw_set) = self.rw_sets.borrow_mut().get_mut(&tg_id) {
            if let Some(rw_val) = tg_rw_set.get(&tuple_id) {
                // if tuple is deleted, it should not be visible in the first place
                assert!(!matches!(RWType::Delete, rw_val));
                assert!(!matches!(RWType::InsDelete, rw_val));
            } else {
                tg_rw_set.insert(tuple_id, RWType::Read);
            }
        } else {
            let mut new_rw_set = HashMap::new();
            new_rw_set.insert(tuple_id, RWType::Read);
            self.rw_sets.borrow_mut().insert(tg_id, new_rw_set);
        }
    }
    pub fn record_insert(&self, location: ItemPointer) {
        !unimplemented!()
    }
    pub fn record_update(&self, location: ItemPointer) {
        !unimplemented!()
    }
}
pub type Oid = u32;
pub type CID = u64;
pub const INVALID_OID: u32 = u32::MAX;
pub const INVALID_TXN_ID: u64 = 0;
pub const MAX_CID: CID = CID::MAX;

#[derive(Copy, Clone)]
pub struct ItemPointer {
    pub block: Oid,
    pub offset: Oid,
}
impl ItemPointer {
    pub fn new(block: Oid, offset: Oid) -> Self {
        ItemPointer { block, offset }
    }
}
pub enum Visibility {
    Invisible,
    Deleted,
    Visible,
}
