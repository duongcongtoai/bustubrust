use crossbeam_channel::{unbounded, Receiver, Sender};
use crossbeam_utils::sync::WaitGroup;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::{
    atomic::{AtomicI32, AtomicU32, Ordering},
    Arc,
};

pub struct Waiter {
    msg_notifier: Sender<DepCode>,
    msg_receiver: Receiver<DepCode>,
    total_dependencies: AtomicU32,
}

pub struct Unlocker {
    // on commit, remove sender from the table
    // so no tx can further send its id to this channel and wait forever
    dep_notifier: Sender<TxID>,

    // find all waiters channel and notify them, whether to abort or to continue
    waiters: Receiver<TxID>,
}

pub struct Tx {
    total_dependencies: AtomicU32,
}
pub type TxID = u64;

pub struct TxManager {
    // inner: DashMap<TxID, Arc<Tx>>,
    // hold address of tx
    commit_dep_senders: DashMap<TxID, RwLock<Sender<TxID>>>,
    commit_dep_resolvers: DashMap<TxID, RwLock<Receiver<DepCode>>>,
}

#[derive(Debug)]
pub enum DepCode {
    Abort,
    Success,
}

fn dosomething() {
    let a = WaitGroup::new();
    let (a, b) = unbounded();
    // a depends on b,c,d
    // b,c success, d fails
    // a block on that and return abort
}

impl TxManager {
    // list of commitsender
    // list of receiver
    //
    fn commit_tx() {
        // get its channel for map
        // drop the sender from the map
        //
        // use its receivers to receive ids of tx that depends on it
        // get receiver's channel and send signal commit/abort to them
        //
        // if the channel is not found, it means they have already aborted, check if sending
        // to the channel return error, if the receiver obj has been dropped
    }

    fn wait_for_dependencies() {
        // each tx before this step has successfully registered itself to its dependencies
        //
        // SAFETY: we guarantee that they will eventually send a signal back to us, or deadlock
        // will occur
        //
        // each tx holds its own receiver channel, gives the sender channel to the tx manager
        //
        // if it find any abort signal, it can abort early, but extra tings need to be considered:
        // - what if other dependencies want to notify an aborted dependee? two case:
        // - dependencies found the receiver's sender channel in the global hashmap, and sends to
        // and aborted tx, will this cause memory leak?
        // - dependencies cannot find the receiver's sender channel, can this be implicitly
        // referred to as this tx has aborted?
        //
        // can use receiver.iter().collect() to block until the senders are dropped
        // but we can cancel early
    }

    fn new_waiter() -> Waiter {
        let (msg_notifier, msg_receiver) = unbounded();
        return Waiter {
            msg_notifier,
            msg_receiver,
            my_wg: Vec::new(),
        };
    }
    fn add_dependencies(&mut self, w: Arc<Unlocker>) {
        let wg = WaitGroup::new();
        w.dep_notifier.send(wg.clone());
        self.my_wg.push(wg);
    }

    fn wait(&self, tx_id: TxID, tx: Tx) {
        let msg_receiver = *self.commit_dep_resolvers.get(&tx_id).unwrap().read();
        let total = tx.total_dependencies.load(Ordering::SeqCst);
        for i in 0..total {
            let code = msg_receiver.recv().unwrap();
            match code {
                DepCode::Abort => {
                    break;
                }
                DepCode::Success => {}
            }
        }
        // Safety: early abort will results in situation that
        // other tx wants to notify this tx about its result, they won't be
        // able to bc we have removed this items, they may be blocked a bit because
        // we are
        msg_receiver.commit_dep_resolvers.remove(&tx_id);
    }
}
