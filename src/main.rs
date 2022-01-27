use bustubrust::bpm::DiskManager;

fn main() {
    let b = DiskManager::new("somefile.db".to_string(), 10);
    println!("{:?}", do_something());
}

fn do_something() -> i32 {
    let a = 1;
    a + 2
}
