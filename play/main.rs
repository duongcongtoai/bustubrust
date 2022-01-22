use std::fs::File;
use std::io;
use std::io::prelude::*;

fn main() -> io::Result<()> {
    let mut f = File::open("foo.txt")?;
    let mut buffer = [0; 10];
    println!("The bytes: {:?}", &buffer[..10]);

    // read up to 10 bytes
    let n = f.read_exact(&mut buffer[..])?;

    println!("The bytes: {:?}", &buffer[..10]);
    Ok(())
}
