mod mvocc;
mod types;
// TODO
// Keyvalue store

pub struct Txn {
    txn_id: u64,
}
pub struct BaseTuple {
    // serve as the version's write lock
    // 0 means tuple is not write locked, when it needs to write lock, CAS(o,txn_id), if CAS fails,
    // abort txn.
    txn_id: u64,

    // txn can reads the tuple if its timestamp is between begin_ts and end_ts
    begin_ts: u64,
    end_ts: u64,
    // addr of the next version
    pointer: u64,
}

/// Use txn_id to pre-compute serialization order
/// txn.read(tuple_id): txn.id between begin_ts, end_ts  
/// txn.write: always update latest version of a tuple:
/// - create tuple A(t+1) if:
///     - not txn holds A(t) write lock
///     - txn.id > A(t).read_ts (no other txn born after this txn has reading this version)
/// - uncommittede insert: tuple.txn_id = current txn_id
/// - on commit: A(t+1) begin_ts and end_ts set to (txn.id,inf), A(t).end_ts = txn.id
pub struct MVTO {}

pub struct MVTOTuple {
    // txn_id of the last txn that reads it
    read_ts: u64,
    // serve as the version's write lock
    // 0 means tuple is not write locked, when it needs to write lock, CAS(o,txn_id), if CAS fails,
    // abort txn.
    txn_id: u64,

    // txn can reads the tuple if its timestamp is between begin_ts and end_ts
    begin_ts: u64,
    end_ts: u64,
    // addr of the next version
    pointer: u64,
}

pub struct MVOCC {}
