use crossbeam_channel::{Receiver, Sender};
use dashmap::{mapref::multiple::RefMulti, DashMap};
use log::debug;
use std::sync::atomic::{AtomicU32, Ordering};

pub struct Tx {
    id: TxID,
    total_dependencies: u64,
    // where i get my dependencies' result
    dep_result_receiver: Receiver<(TxID, DepCode)>,
    // where i get my dependent
    dep_registrations: Receiver<TxID>,
}
pub type TxID = u64;

unsafe impl Sync for TxManager {}
unsafe impl Send for TxManager {}

pub struct TxManager {
    commit_dep_senders: DashMap<TxID, Sender<TxID>>,
    commit_dep_result_sender: DashMap<TxID, Sender<(TxID, DepCode)>>,
}

#[derive(Debug, Copy, Clone)]
pub enum DepCode {
    Abort,
    Success,
}

impl TxManager {
    fn new() -> Self {
        TxManager {
            commit_dep_senders: DashMap::new(),
            commit_dep_result_sender: DashMap::new(),
        }
    }
    // this tx_id is dependent on some other tx, and would like those tx to notify it using this
    // sender channel
    // also, other tx may also are dependent on this tx_id, for them to register their
    // dependencies, this tx_id also need to expose a sender channel
    fn register_channel(
        &self,
        tx_id: TxID,
        dep_sender: Sender<TxID>,
        dep_result_sender: Sender<(TxID, DepCode)>,
    ) {
        self.commit_dep_senders.insert(tx_id, dep_sender);
        self.commit_dep_result_sender
            .insert(tx_id, dep_result_sender);
    }

    fn add_dep(&self, tx_id: TxID, depend_on: TxID) -> bool {
        let maybe_registerer = self.commit_dep_senders.get(&depend_on);

        match maybe_registerer {
            Some(sender) => {
                let mut success = true;
                match sender.send(tx_id) {
                    Ok(_) => {}
                    Err(msg) => {
                        success = false;
                        debug!("tx_id {} failed to register its dependencies with tx_id {} because the receiver may have been droped",
                    tx_id,depend_on);
                    }
                }
                return success;
            }
            None => {
                return false;
            }
        };
    }

    fn announce_tx_result(&self, tx: Tx, announce_code: DepCode) {
        // no other tx can register me as their dependencies
        // if they somehow acquire me after this code runs, they still hold a reference to one copy
        // of sender obj, then my next code will block until that copied sender is dropped :D
        self.commit_dep_senders.remove(&tx.id).unwrap();
        // get all the dependencies and notify them about my result
        loop {
            let new_dep = tx.dep_registrations.recv();
            match new_dep {
                Ok(tx_id) => {
                    match self.commit_dep_result_sender.get(&tx_id) {
                        // try telling this tx about its result
                        Some(sender) => match sender.send((tx.id, announce_code)) {
                            Ok(()) => {}
                            // this op may fail, because between the period this thread acquire the
                            // sender obj and actually sending it, the thread holding the receiver
                            // may have dropped the receiver obj, log it out first
                            Err(some_err) => {
                                debug!(
                                    "tx_id {} failed to notify ts dependent tx_id {}",
                                    tx.id, tx_id
                                );
                            }
                        },
                        None => {
                            // this tx has removed its own channel due to early abort, we don't need to announce
                        }
                    }
                }
                Err(_) => {
                    // SAFETY: we need this, we expect this loop to end once all the senders have
                    // dropped referencing, that's the only guarantee to have no leftover message
                    break;
                }
            }
        }
    }

    fn wait_and_announce(&self, tx: Tx) {
        // each tx before this step has successfully registered itself to its dependencies
        //
        // SAFETY: we guarantee that they will eventually send a signal back to us, or deadlock
        // will occur
        //
        // each tx holds its own receiver channel, gives the sender channel to the tx manager
        //
        // if it find any abort signal, it can abort early, but extra things need to be considered:
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

        // SAFETY: early abort will results in situation that
        // other dep_tx wants to notify this tx about its result, they won't be
        // able to bc we have removed this items, need to check crossbeam channel behaviour what
        // happens if such event occured
        // ------------------------------------------------------
        // other tx does not need to notify me about their result anymore, in case i abort
        // they can ignore error and continue
        self.commit_dep_result_sender.remove(&tx.id).unwrap();
        let mut announce_code = DepCode::Abort;
        if do_commit {
            announce_code = DepCode::Success;
        }
        self.announce_tx_result(tx, announce_code);
    }
}
impl Tx {
    fn new(
        id: TxID,
        dep_result_receiver: Receiver<(u64, DepCode)>,
        dep_registrations: Receiver<TxID>,
    ) -> Self {
        Tx {
            id,
            dep_result_receiver,
            dep_registrations,
            total_dependencies: 0,
        }
    }
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

#[cfg(test)]
mod tests {
    use super::{DepCode, Tx, TxID, TxManager};
    use crossbeam_channel::{unbounded, Receiver};
    use std::{sync::Arc, thread, time::Duration};

    fn register_tx(mgr: &Arc<TxManager>, tx_id: TxID) -> Tx {
        // tx1
        let (dep_sender, dep_recv) = unbounded();
        let (dep_result_sender, dep_result_recv) = unbounded();
        mgr.register_channel(tx_id, dep_sender, dep_result_sender);
        Tx::new(tx_id, dep_result_recv, dep_recv)
    }

    #[test]
    fn chaining_doubble_dependencies() {
        let mgr = Arc::new(TxManager::new());
        let tx1 = register_tx(&mgr, 1);
        let tx2 = register_tx(&mgr, 2);

        let mut joinhandles = vec![];

        let mut last_tx = (1, 2);
        // tx in 1 group depends on the same txid
        for group in 2..5 {
            for offset in 1..5 {
                let mgrcloned = mgr.clone();
                let this_tx_id = group * 10 + offset;
                let mut tx = register_tx(&mgrcloned, this_tx_id);
                assert!(mgrcloned.add_dep(this_tx_id, last_tx.0));
                assert!(mgrcloned.add_dep(this_tx_id, last_tx.1));

                let t = thread::spawn(move || {
                    tx.total_dependencies = 2;
                    mgrcloned.wait_and_announce(tx);
                });
                joinhandles.push(t);
            }
            last_tx = (group * 10 + 1, group * 10 + 2);
        }
        thread::sleep(Duration::from_secs(1));
        mgr.wait_and_announce(tx1);
        mgr.wait_and_announce(tx2);

        for t in joinhandles.into_iter() {
            t.join().unwrap();
        }
    }
}
