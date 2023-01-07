/* pub fn main{
let value: u64 = 0b1111_0000_0000_0000_0000_0000_0000_0000;
let msb = value.leading_zeros() as u64;

assert_eq!(msb, 60);
} */

fn main() {
    let value: u64 =
        0b1111_0000_0000_0000_0000_0000_0000_1111_1111_0000_0000_0000_0000_0000_0000_1111;
    let n = u32::MAX >> 2;
    let msb = value.leading_zeros() as u64;
    let msb2 = value.leading_ones() as u64;
    println!("{msb} {msb2} {value} {}", n.leading_ones());
    // assert_eq!(msb, 60);
    assert_eq!(msb2, 60);
}
