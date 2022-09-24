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
    state: TxPhase,
}
pub enum TxPhase {
    Processing,
    Preparing,
    Comitted,
    Aborted,
}

pub struct OCCTx {
    read_set: Vec<Tuple>,
    // info needed to repeat scan, maybe if i scan this again, those things must be visible again,
    // you some weird committed items
    scan_set: Vec<()>, // I don't know its type yet
    // before/after updated versions
    // deleted version
    // inserted version
    write_set: Vec<Tuple>,
}
pub trait Predicate {}

pub struct IndexScanOp<P: Predicate> {
    read_time: u64,
    index: Vec<()>,
    predicate: P,
}

// TODO: make this generic
impl MVOCC {
    fn txn_commit(tx: &mut Tx) {}

    fn txn_abort(tx: &mut Tx) {}

    fn txn_processing_phase_index_scan(tx: &mut Tx) {}

    /// Preparation phase. During this phase the transaction determines whether it can commit or is forced to abort. If it has to
    /// abort, it switches its state to Aborted and continues to the next
    /// phase. If it is ready to commit, it writes all its new versions and
    /// information about deleted versions to a redo log record and
    /// waits for the log record to reach non-volatile storage. The
    /// transaction then switches its state to Committed.
    fn txn_prepare_phase(tx: &mut Tx) {}
    // on error must switch state to abort
    fn txn_processing_phase_update(
        &self,
        tx: &mut Tx,
        key: String,
        payload: Vec<u8>,
    ) -> Result<(), String> {
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

pub fn register_commit_dep(tx: &Tx) {
    struct TxDepender {
        // how many unresolved commit dependencies it still has (this tx waits for other to commit)
        // I can't commit until this number is 0
        commit_dep_counter: u64,
        abort_now: bool,
    }

    struct TxDependee {
        // on my commit, i must decr commit_dep_counter of every txn in this set
        // on my abort, i must set abort_now of every txn in this set
        commit_dep_set: Vec<u64>,
    }
}

/// Safety: other thread may also looking at this version chain, somehow make this function
/// threadsafe
/// the return version ts must be valid ts (not any tx_id)
/// must return the end_ts of the version at the time we found it
/// I don't know if we find a version that has a tx_id, do we abort or wait for it to set_back a
/// visible timestamp
///
/// Read more in PAPER:
/// While determining the visibility of a version is straightforward in
/// principle, it is more complicated in practice as we do not want a
/// transaction to block (wait) during normal processing. Recall that a
/// version’s Begin or End fields can temporarily store a transaction
/// ID, if the version is being updated. If a reader encounters such a
/// version, determining visibility without blocking requires checking
/// another transaction’s state and end timestamp and potentially even
/// restricting the serialization order of transactions.
/// Note that the returned number can either be ts or id of a txn
pub fn find_visible_version(
    tx: &Tx,
    read_ts: u64,
    versions: &VersionChain,
) -> Option<(Tuple, u64)> {
    unimplemented!()
    // begin_ts == tx_id:
    // 1. if tx_id == my_ts && end_ts == inf => visible

    // 2. tx_id == other tx_b'sid
    // 2.a. tx_b is preparing
    // use tx_id as begin_ts and run test_visibility(...) , if true => allow speculatively read
    // WHAT IS SPECULATIVELY READ AND WRITE/IGNORE THO
    // 2.b. tx_b is committed
    // use tx_id as begin_ts and run test_visibility(...)
    // 2.c. tx_b is aborted => ignore
    // 2.d. tx_b is terminated or not found => re_read tx.begin, it can be finalized only if it has
    // recently finalize the begin field of this tx

    // Acquire commit dependency on other Tx:

    // end_ts == tx_id:
    // 1. if tx_id == my_ts => visible
    // 2. txn_id = other tx_e's id and tx_e's is preparing
    // 2.a. tx_e's end_ts > read_ts => visible
    // 2.b. tx_e's end_ts < read_ts => speculatively ignore
    // 3. tx_e is committed: use tx_e's end_ts and v's end_ts to check visibility
    // 4. ts_e is aborted => visible
    // 5. ts_e is terminated or not found => re_read end_ts again .
}

/// Suppose transaction T wants to update a version V. V is updatable
/// only if it is the latest version, that is, it has an end timestamp equal
/// to infinity or its End field contains the ID of a transaction TE and
/// TE’s state is Aborted. If the state of transaction TE is Active or
/// Preparing, V is the latest committed version but there is a later
/// uncommitted version. This is a write-write conflict. We follow the
/// first-writer-wins rule and force transaction T to abort.
unsafe fn update(tx: &Tx, end_ts: u64, t: Tuple, new_value: Vec<u8>) -> Option<Tuple> {
    // if end_ts == inf, okay
    // if it is a txn_id => check if this txn is aborted or not found

    // load its ts into this tuple's end ts
    let new_ts: u64 = tx.begin_ts as u64 | 1 << 32;
    let acquired = t
        .end
        .compare_exchange(end_ts, new_ts, Ordering::SeqCst, Ordering::SeqCst);
    // cas fail, abort, some other tx has acquired
    if let Err(_) = acquired {
        return None;
    }
    let new_tuple = t.new_version(AtomicU64::new(new_ts), new_value);
    return Some(new_tuple);
}
