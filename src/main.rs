use bustubrust::bpm::DiskManager;

fn main() {
    let b = DiskManager::new("somefile.db".to_string(), 10);
}
