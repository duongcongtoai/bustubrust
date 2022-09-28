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

pub struct OccTx {
    read_set: Vec<Tuple>,
    // info needed to repeat scan, maybe if i scan this again, those things must be visible again,
    // you some weird committed items
    scan_set: Vec<()>, // I don't know its type yet
    // before/after updated versions
    // deleted version
    // inserted version
    write_set: Vec<Tuple>,
}
pub struct Predicate {
    search: SearchPred,
    residual: ResidualPred,
}

pub enum SearchPred {
    Equality(EqualityPred),
    Range(RangePred),
}
pub struct EqualityPred {
    field: String,
}
pub struct RangePred {
    // TODO inclusive or exclusive
    from: String,
    to: String,
}
pub struct ResidualPred {}

pub struct IndexScanOp {
    read_time: u64,
    index: Vec<()>,
    predicate: Predicate,
}

/// TODO: make this generic
/// How to handle commit dependencies announcement?
impl MVOCC {
    fn txn_commit(tx: &mut Tx) {}

    fn txn_abort(tx: &mut Tx) {}

    /// with serializable isolation level
    /// read_time = tx.begin_ts
    /// 1. start scan: register to t's scanset => t can check for phantom during validation
    /// must store info to check if the scan is repeatable: index, predicates
    ///
    /// 2. check predicate: if version does not satisfy P, it is ignored. If the scan is range
    ///    scan and index key exceeds the upper bound of the range => terminated
    ///
    /// 3. check visibility, it may results into some commit dependencies with other tx, => it
    ///    must register its dependencies to those tx. IF the visibility test fails => continue
    ///    with next version
    /// 4. if T intends just read V, it add V to its readset
    /// 5. If T intends to update or delete V, must check if version is updatable (check the
    ///    update section's note). note that it can still has speculative update (it writes to
    ///    some result of a uncomitted tx, but this tx must have finished its processing phase)
    /// 6. cas v's end's field with its tx_id, if fails => abort. Else this serves as an
    ///    exclusive write lock, it records 2 pointer to its write set: old/new version. Those
    ///    are used for multiple purpose (logging new version during commit), post processing
    ///    after commit/abort, locate old version for later gc
    ///    New version is not visible until t finish processing phase, => feel free to add new
    ///    version
    /// 7. a delete = update without creating new version. => case like update case, pointer is
    ///    added to T write set and complete
    ///
    /// 8. precommit: acquire end_ts for T and set state to Preparing
    fn txn_processing_phase_index_scan(tx: &mut Tx) {}

    /// read validation, wait for commit depenencies, logging
    fn txn_preparing_phase_index_scan(tx: &mut Tx) {
        // validate
        // wait for commit dependencies
        // logging
    }

    /// This steps may add more commit dependencies to T
    /// 1. check read visibility: for each of its readset, check visibility(version,tx.end_ts)
    /// 2. check phantom: for each scanset, repeat scan looking for versions came into existence
    ///    during T lifetime and are visible at the end of tx (read_ts is now t.end_ts)
    /// Check the figure in the paper for more details
    /// 1. V is visible before and after T => pass read, pass phantom
    /// 2. V is visible before but not visible for T's end_ts => fail read, pass phantom
    /// 3. V is not visible before T, but later added but is made invisible again before T's end =>
    ///    pass => read NA, phantom pass
    /// 4. V is not visible before T but later visible for T's end_ts => read NA, but phantom fail
    /// If either read/phatom validation fails, it must abort
    fn txn_preparing_phase_index_scan_validate(tx: &mut Tx) {}

    /// scan writeset, write new versions created to persistent log, commit order based on tx's
    /// end_ts, wait for the commit is flushed to disk, then set phase=postprocessing
    fn txn_preparing_phase_index_scan_logging(tx: &mut Tx) {}

    /// Commit: Propagate its end_ts to old version's end_ts and new version's begin_ts
    /// Abort: begin of new version set to inf, end_field of old version to inf. However, if it
    /// dectects that end_field of old version has changed to something else, leave it
    ///
    /// Broadcast commit depdencies
    ///
    /// Remove this Tx from Tx table, but pointers to old version may be needed for gc
    fn txn_postprocessing_phase(tx: &mut Tx) {}

    /// Preparation phase. During this phase the transaction determines whether it can commit or is forced to abort. If it has to
    /// abort, it switches its state to Aborted and continues to the next
    /// phase. If it is ready to commit, it writes all its new versions and
    /// information about deleted versions to a redo log record and
    /// waits for the log record to reach non-volatile storage. The
    /// transaction then switches its state to Committed.
    fn txn_prepare_phase(tx: &mut Tx) {
        // Backward validation instead of forward validation:
        // Check if committing transaction conflicts with any previously committed tx
        //
        // Previous approach: validate readset with writeset of other tx(things it has been read
        // has not been written by other tx)
        // => optimization: things it has been read is still visible as of the end of the tx
    }
    // on error must switch state to abort
    fn txn_processing_phase_update(
        &self,
        tx: &mut Tx,
        key: String,
        payload: Vec<u8>,
    ) -> Result<(), String> {
        let versions = self.index.get_mut(&key).unwrap();

        // Based on isolation level, this ts may differ, but let's make it default serializable and
        // use tx.begin_ts
        let read_ts = tx.begin_ts;

        match find_visible_version(tx, read_ts as u64, &versions) {
            None => return Err("not found visible version".to_string()),
            Some(visibility_ret) => {
                match visibility_ret {
                    Err(some_err) => return Err(some_err),
                    Ok((version, last_read_ts)) => unsafe {
                        let update_ret = update(tx, last_read_ts, version, payload);
                        match update_ret {
                            // Safety: we have aleady acquired write lock on the
                            Some(new_version) => {}
                            None => return Err("serialization err".to_string()),
                        };
                    },
                }
            }
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
) -> Option<Result<(Tuple, u64), String>> {
    for v in versions {}

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
    // 2.b. tx_e's end_ts < read_ts => speculatively ignore, and is commit dependent of tx_e
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
