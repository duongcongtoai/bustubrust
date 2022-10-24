use byteorder::{BigEndian, ByteOrder};

fn main() {
    let mut t = Tuple { data: vec![0; 16] };
    let sl = &mut t.data[8..];
    BigEndian::write_i32(sl, 10);
    println!("{:?}", t.data);
}
pub struct Tuple {
    data: Vec<u8>,
}
