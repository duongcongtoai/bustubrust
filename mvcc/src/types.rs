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
}

impl Tx {
    pub fn record_update(&self, location: ItemPointer) {
        !unimplemented!()
    }
}
pub type Oid = u32;
pub const INVALID_OID: u32 = u32::MAX;

#[derive(Copy, Clone)]
pub struct ItemPointer {
    pub block: Oid,
    offset: Oid,
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
