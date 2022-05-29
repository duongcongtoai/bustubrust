use lock_api::RawMutex as ApiRawMutex;
use parking_lot::RawMutex;
use std::collections::{HashMap, VecDeque};

use crate::sql::{
    common::RID,
    tx::{IsolationLevel, TwoPLState, Txn},
};
pub struct LockRequestQueue {
    latch: RawMutex,
    queue: VecDeque<LockRq>,
}
pub struct LockRq {
    txn_id: u32,
    lock_mode: LockMode,
    granted: bool,
}
pub enum LockMode {}
pub struct TxMgr {}

pub struct LockMgr {
    latch: RawMutex,
    lock_table: HashMap<RID, LockRequestQueue>,
}

impl LockMgr {
    fn lock_s(&mut self, txn: &mut Txn, rid: RID) -> bool {
        match txn.isolation_level() {
            IsolationLevel::ReadUncommitted => {
                txn.abort();
                return false;
            }
            IsolationLevel::RepeatableRead => {
                if txn.state() == TwoPLState::Shrinking {
                    txn.abort();
                    return false;
                }
            }
            _ => {}
        };
        if txn.s_locked(rid) || txn.x_locked(rid) {
            return true;
        }
        // can this be empty
        let queue = self.get_queue(rid).unwrap();
    }
    fn lock_x(_: Txn, _: RID) {}
    fn upgrade_lock(_: Txn, _: RID) {}
    fn unlock(_: Txn, _: RID) {}

    fn get_queue(&self, rid: RID) -> Option<&LockRequestQueue> {
        // let latch = self.latch.lock();
        self.latch.lock();
        let queue = self.lock_table.get(&rid);
        unsafe {
            self.latch.unlock();
        }
        queue
    }
}

/* class LockManager {
  enum class LockMode { SHARED, EXCLUSIVE };

  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

    txn_id_t txn_id_;
    LockMode lock_mode_;
    bool granted_;
  };

  class LockRequestQueue {
   public:
    std::list<LockRequest> request_queue_;
    std::condition_variable cv_;  // for notifying blocked transactions on this rid
    bool upgrading_ = false;
  };

 public:
  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() {
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
    LOG_INFO("Cycle detection thread launched");
  }

  ~LockManager() {
    enable_cycle_detection_ = false;
    cycle_detection_thread_->join();
    delete cycle_detection_thread_;
    LOG_INFO("Cycle detection thread stopped");
  }

  /*
   * [LOCK_NOTE]: For all locking functions, we:
   * 1. return false if the transaction is aborted; and
   * 2. block on wait, return true when the lock request is granted; and
   * 3. it is undefined behavior to try locking an already locked RID in the same transaction, i.e. the transaction
   *    is responsible for keeping track of its current locks.
   */

  /**
   * Acquire a lock on RID in shared mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the shared lock
   * @param rid the RID to be locked in shared mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockShared(Transaction *txn, const RID &rid);

  /**
   * Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the exclusive lock
   * @param rid the RID to be locked in exclusive mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockExclusive(Transaction *txn, const RID &rid);

  /**
   * Upgrade a lock from a shared lock to an exclusive lock.
   * @param txn the transaction requesting the lock upgrade
   * @param rid the RID that should already be locked in shared mode by the requesting transaction
   * @return true if the upgrade is successful, false otherwise
   */
  bool LockUpgrade(Transaction *txn, const RID &rid);

  /**
   * Release the lock held by the transaction.
   * @param txn the transaction releasing the lock, it should actually hold the lock
   * @param rid the RID that is locked by the transaction
   * @return true if the unlock is successful, false otherwise
   */
  bool Unlock(Transaction *txn, const RID &rid);

  /*** Graph API ***/
  /**
   * Adds edge t1->t2
   */

  /** Adds an edge from t1 -> t2. */
  void AddEdge(txn_id_t t1, txn_id_t t2);

  /** Removes an edge from t1 -> t2. */
  void RemoveEdge(txn_id_t t1, txn_id_t t2);

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  bool HasCycle(txn_id_t *txn_id);

  /** @return the set of all edges in the graph, used for testing only! */
  std::vector<std::pair<txn_id_t, txn_id_t>> GetEdgeList();

  /** Runs cycle detection in the background. */
  void RunCycleDetection();

 private:
  std::mutex latch_;
  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;

  /** Lock table for lock requests. */
  std::unordered_map<RID, LockRequestQueue> lock_table_;
  /** Waits-for graph representation. */
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
} */
