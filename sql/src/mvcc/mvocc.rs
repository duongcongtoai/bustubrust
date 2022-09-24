use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

/* pub struct Ts {
    tx_id: u64,
    ts: u64,
} */

// first bit: 1: is tx_id, 0: is commit_ts
pub type Ts = AtomicU64;

// for inmemory, we assume that this is empty, we create a linked list of versions anyway, so next version
// is always the next item
pub struct Tuple {
    begin: Ts,
    end: Ts,
    payload: Vec<u8>,
    hash_ptr: u64,
}
impl Tuple {
    fn new_version(&self, begin: Ts, payload: Vec<u8>) -> Tuple {
        Tuple {
            begin,
            payload,
            end: AtomicU64::new(0),
            hash_ptr: 0,
        }
    }

    fn new_version_from_tx(&self, tx: Tx, begin: Ts, payload: Vec<u8>) -> Tuple {
        Tuple {
            begin: AtomicU64::new(1 << 32 | tx.begin_ts as u64),
            payload,
            end: AtomicU64::new(0),
            hash_ptr: 0,
        }
    }
}

type VersionChain = Vec<Tuple>;

pub struct MVOCC {
    index: DashMap<String, VersionChain>,
}
pub struct Tx {
    begin_ts: u32,
    state: TxState,
}
pub enum TxState {
    Active,
    Preparing,
    Comitted,
    Aborted,
}

// TODO: make this generic
impl MVOCC {
    fn update(&self, tx: &mut Tx, key: String, payload: Vec<u8>) -> Result<(), String> {
        let versions = self.index.get_mut(&key).unwrap();
        match find_visible_version(tx, &versions) {
            None => return Err("not found visible version".to_string()),
            Some((version, last_read_ts)) => unsafe {
                let ret = update(tx, last_read_ts, version, payload);
                match ret {
                    // Safety: we have aleady acquired write lock on the
                    Some(new_version) => {}
                    None => return Err("serialization err".to_string()),
                };
            },
        }

        Ok(())
    }
}

// Safety: other thread may also looking at this version chain, somehow make this function
// threadsafe
// the return version ts must be valid ts (not any tx_id)
// must return the end_ts of the version at the time we found it
// I don't know if we find a version that has a tx_id, do we abort or wait for it to set_back a
// visible timestamp
pub fn find_visible_version(tx: &Tx, versions: &VersionChain) -> Option<(Tuple, u64)> {
    unimplemented!()
}

/// TODO:
/// Tx 75 transfer 20 from Larry to John (creating 2 new versions for Larry and John)
/// A transaction ID stored in the End field serves as a write lock and prevents other
/// transactions from updating the same version and it identifies
/// which transaction has updated it. A transaction Id stored in the
/// Begin field informs readers that the version may not yet be committed and it identifies which transaction owns the version
///
/// 20, (tx75)
unsafe fn update(tx: &Tx, last_read_ts: u64, t: Tuple, new_value: Vec<u8>) -> Option<Tuple> {
    // load its ts into this tuple's end ts
    let new_ts: u64 = tx.begin_ts as u64 | 1 << 32;
    let acquired = t
        .end
        .compare_exchange(last_read_ts, new_ts, Ordering::SeqCst, Ordering::SeqCst);
    // cas fail, abort, some other tx has acquired
    if let Err(_) = acquired {
        return None;
    }
    let new_tuple = t.new_version(AtomicU64::new(new_ts), new_value);
    return Some(new_tuple);
}
