use crossbeam_channel::{unbounded, Receiver, Sender};

fn main() {
    let (s, r) = unbounded();
    println!("{}", 1);
}
