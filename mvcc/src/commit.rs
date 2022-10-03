use crossbeam_channel::{Receiver, Sender};
use dashmap::DashMap;
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

pub struct TxManager {
    // inner: DashMap<TxID, Arc<Tx>>,
    // hold address of tx
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

    fn add_dep(&self, tx_id: TxID, dep_id: TxID) -> bool {
        let sender = self.commit_dep_senders.get(&dep_id).unwrap();
        let mut success = false;
        match sender.send(tx_id) {
            Ok(_) => {}
            Err(msg) => {
                success = false;
                debug!("tx_id {} failed to register its dependencies with tx_id {} because the receiver may have been droped",
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

        // must announce its dependencies about its result
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
    fn simple_wait_for_sleeping_tx() {
        let mgr = Arc::new(TxManager::new());
        let mut tx1 = register_tx(&mgr, 1);
        let tx2 = register_tx(&mgr, 2);
        // tx1 depends on tx2
        mgr.add_dep(1, 2);
        tx1.total_dependencies = 1;
        let t1 = thread::spawn(move || {
            println!("tx1 waiting");
            tx1._wait_for_dep_dependencies();
            println!("tx1 done waiting");
        });
        let mgrclone1 = mgr.clone();
        let mgrclone2 = mgr.clone();
        let t2 = thread::spawn(move || {
            let mut tx3 = register_tx(&mgrclone1, 3);
            mgrclone1.add_dep(3, 2);
            println!("tx3 waiting");
            tx3.total_dependencies = 1;
            tx3._wait_for_dep_dependencies();
            println!("tx3 done waiting");
        });
        let t3 = thread::spawn(move || {
            // to something and commit
            println!("tx2 sleeping");
            thread::sleep(Duration::from_secs(3));
            println!("tx2 done sleeping");
            mgrclone2.wait_for_dependencies(tx2);
        });
        t1.join().unwrap();
        t2.join().unwrap();
        t3.join().unwrap();
    }
}
