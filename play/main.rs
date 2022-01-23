use ghost_cell::{GhostCell, GhostToken};
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::sync::Arc;
use std::sync::Mutex;

fn main3() -> io::Result<()> {
    let mut f = File::open("foo.txt")?;
    let mut buffer = [0; 10];
    println!("The bytes: {:?}", &buffer[..10]);

    // read up to 10 bytes
    let n = f.read_exact(&mut buffer[..])?;

    println!("The bytes: {:?}", &buffer[..10]);
    Ok(())
}

fn main4() {
    let n = 42;

    let value = GhostToken::new(|mut token| {
        let cell = GhostCell::new(42);

        let vec: Vec<_> = (0..n).map(|_| &cell).collect();

        let st = vec[n / 2].borrow_mut(&mut token);

        *st = 13;

        *cell.borrow_mut(&mut token)
    });

    assert_eq!(33, value);
}

fn main() {
    main2()
}
struct Container {
    frames: Vec<Arc<Mutex<String>>>,
}

fn main2() {
    let mut frames = Vec::new();
    let some_p: &mut Arc<Mutex<String>>;
    for i in 0..10 {
        frames.push(Arc::new(Mutex::new(format!("{}", i))));
    }
    let c = Container { frames: frames };
    let muc = Mutex::new(c);
    let mut locked_c = muc.lock().unwrap();

    // let some_frame = &frames[0];
    unsafe {
        some_p = &mut *(&mut locked_c.frames[0] as *mut Arc<Mutex<String>>);
    };

    let locked_frame = Arc::clone(&*some_p);
    let mut st = locked_frame.lock().unwrap();

    drop(locked_c);
    println!(
        "can still use child mutex after dropping parent mutex yay!!!! {}",
        st,
    );
    st.push_str("yoo");
    drop(st);
    let st2 = muc.lock().unwrap();
    println!("{:?}", st2.frames);
}
