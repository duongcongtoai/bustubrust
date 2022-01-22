use dashmap::DashMap;
use libc::O_DIRECT;
use std::collections::VecDeque;
use std::os::unix::fs::OpenOptionsExt;
use std::sync::Mutex;
use std::{
    fs::{File, OpenOptions},
    hash::Hasher,
    io::{Error, Read, Seek, SeekFrom, Write},
};

struct BufferPool<R: Replacer> {
    frames: Vec<Frames>,
    page_table: DashMap<i64, &'static Frames>,
    size: i64,

    next_new: i64,
    replacer: R,
    free_list: VecDeque<FrameID>,
}
type FrameID = i64;
type PageID = i64;

trait Replacer {
    fn victim(&self) -> (FrameID, bool);

    // frameID should not be victimized until unpin
    fn pin(&self, frame_id: FrameID);

    // allow frame to be victimizedable
    fn unpin(&self, frame_id: FrameID);

    // items that can be victimized
    fn size(&self) -> i64;
}

struct Frames {
    id: FrameID,
    page_id: PageID,
}

pub struct DiskManager {
    f: Mutex<File>,
    page_size: usize,
}
pub struct StrErr {
    root: String,
}
impl StrErr {
    fn new(st: String) -> Self {
        StrErr { root: st }
    }
}

impl std::convert::From<Error> for StrErr {
    fn from(e: Error) -> Self {
        StrErr {
            root: e.to_string(),
        }
    }
}

impl DiskManager {
    pub fn new(filepath: String, page_size: u64) -> Self {
        File::create(filepath.clone()).expect("io error");
        let mut opts = OpenOptions::new();
        opts.write(true).read(true).create(true).mode(0o666);
        if cfg!(unix) {
            opts.custom_flags(O_DIRECT);
        }
        let f = opts.open(filepath).unwrap();
        return DiskManager {
            f: Mutex::new(f),
            page_size: (page_size as usize),
        };
    }

    pub fn read_into_frame(&self, page_id: PageID, buf: &mut [u8]) -> Result<(), StrErr> {
        let mut f = self.f.lock().unwrap();
        f.seek(SeekFrom::Start(page_id as u64 * self.page_size as u64))?;
        let read_bytes = f.read(&mut buf[..self.page_size])?;
        if read_bytes != self.page_size {
            return Err(StrErr::new("not enough byte read".to_string()));
        }
        Ok(())
    }

    pub fn write_from_frame_to_file(&self, page_id: PageID, buf: &mut [u8]) -> Result<(), StrErr> {
        if buf.len() != self.page_size {
            return Err(StrErr::new("frame has invalid length".to_string()));
        }
        let mut f = self.f.lock().unwrap();
        f.seek(SeekFrom::Start(page_id as u64 * self.page_size as u64))?;
        let mut f = self.f.lock().unwrap();
        let byte_written = f.write(buf)?;
        if byte_written != self.page_size {
            return Err(StrErr::new("invalid bytes written".to_string()));
        }
        File::sync_all(&mut f)?;
        Ok(())
    }
}
