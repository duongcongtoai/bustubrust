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
