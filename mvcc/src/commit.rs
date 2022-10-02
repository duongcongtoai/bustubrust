use crossbeam_channel::{Receiver, Sender};
use dashmap::DashMap;
use log::debug;
use std::sync::atomic::{AtomicU32, Ordering};

pub struct Tx {
    id: TxID,
    total_dependencies: u64,
    dep_result_receiver: Receiver<(TxID, DepCode)>,
    dep_receiver: Receiver<TxID>,
}
pub type TxID = u64;

pub struct TxManager {
    // inner: DashMap<TxID, Arc<Tx>>,
    // hold address of tx
    commit_dep_senders: DashMap<TxID, Sender<TxID>>,
    commit_dep_result_sender: DashMap<TxID, Sender<(TxID, DepCode)>>,
}

#[derive(Debug)]
pub enum DepCode {
    Abort,
    Success,
}

impl TxManager {
    // this tx_id is dependent on some other tx, and would like those tx to notify it using this
    // sender channel
    // also, other tx may also are dependent on this tx_id, for them to register their
    // dependencies, this tx_id also need to expose a sender channel
    fn register_channel(
        &self,
        tx_id: TxID,
        dep_sender: Sender<TxID>,
        depee_sender: Sender<(TxID, DepCode)>,
    ) {
        self.commit_dep_senders.insert(tx_id, dep_sender);
        self.commit_dep_result_sender.insert(tx_id, depee_sender);
    }

    fn add_dep(&self, tx_id: TxID, dep_id: TxID) -> bool {
        let sender = self.commit_dep_senders.get(&dep_id).unwrap();
        let mut success = false;
        match sender.send(tx_id) {
            Ok(_) => {}
            Err(msg) => {
                success = false;
                debug!("tx_id {} failed to register its dependencies with tx_id {} because of receiver may have been droped",
                    tx_id,dep_id);
            }
        }
        return success;

        // get its channel for map
        // drop the sender from the map
        //
        // use its receivers to receive ids of tx that depends on it
        // get receiver's channel and send signal commit/abort to them
        //
        // if the channel is not found, it means they have already aborted, check if sending
        // to the channel return error, if the receiver obj has been dropped
    }

    fn wait_for_dependencies(&self, tx: Tx) {
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
        //
        let do_commit = tx._wait_for_dep_dependencies();

        // Safety: early abort will results in situation that
        // other dep_tx wants to notify this tx about its result, they won't be
        // able to bc we have removed this items, need to check crossbeam channel behaviour what
        // happnes if such event occured
        self.commit_dep_result_sender.remove(&tx.id).unwrap();
    }
}
impl Tx {
    fn _wait_for_dep_dependencies(&self) -> bool {
        let msg_receiver = &self.dep_result_receiver;
        let total = self.total_dependencies;

        let mut abort = false;
        for i in 0..total {
            let (dep_id, code) = msg_receiver.recv().unwrap();
            match code {
                DepCode::Abort => {
                    abort = true;
                    debug!(
                        "Tx {} early abort because of dependency on {} aborted",
                        self.id, dep_id,
                    );
                    break;
                }
                DepCode::Success => {}
            }
        }
        if !abort {
            debug!(
                "Tx {} commit after waiting for {} of its dependencies",
                self.id, total
            );
        }

        return !abort;
    }
}
