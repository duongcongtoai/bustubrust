use core::cell::RefCell;
use libc::O_DIRECT;
use owning_ref::OwningHandle;
use parking_lot::{Mutex, MutexGuard};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::os::unix::fs::OpenOptionsExt;
// use std::fs::Metadata;
use std::sync::Arc;
use std::{
    fs::{File, OpenOptions},
    hash::Hasher,
    io::{copy, empty, Error, Read, Seek, SeekFrom, Write},
};
pub struct BufferPoolManager<R: Replacer> {
    bp: Mutex<BufferPool<R>>,
    pub dm: DiskManager,
}
/* struct AutoDropFrame<F: Fn(BufferPoolManager<R>), R>
where
    R: Replacer,
{
    arc: Arc<Mutex<Frame>>,
    drop_func: F,
    r: PhantomData<R>,
}

impl<F, R> Drop for AutoDropFrame<F, R>
where
    F: Fn(BufferPoolManager<R>),
    R: Replacer,
{
    fn drop(&mut self) {
        todo!()
    }
} */

impl<R> BufferPoolManager<R>
where
    R: Replacer,
{
    pub fn new(max_size: usize, r: R, dm: DiskManager) -> Self {
        let bp = BufferPool::new(max_size, r);
        BufferPoolManager {
            bp: Mutex::new(bp),
            dm,
        }
    }

    pub fn new_page(&self) -> Result<Arc<Mutex<Frame>>, StrErr> {
        let dm = &self.dm;
        BufferPool::new_page(&self.bp, dm)
    }

    pub fn delete_page_locked<'a>(
        &self,
        item: OwningHandle<Arc<Mutex<Frame>>, MutexGuard<'a, Frame>>,
    ) -> Result<(), StrErr> {
        return BufferPool::delete_page_locked(&self.bp, item);
    }

    pub fn fetch_page(&self, page_id: PageID) -> Result<Arc<Mutex<Frame>>, StrErr> {
        BufferPool::fetch_page(&self.bp, &self.dm, page_id)
    }

    #[cfg(feature = "testing")]
    pub fn assert_clean_frame(&self, exceptions: &[i64]) {
        let mu = self.bp.lock();
        let mut frames = mu.frames.borrow_mut();
        'loop1: for f in frames.iter_mut() {
            let locked_fr = f.lock();
            let frame_id = locked_fr.id;
            let page_id = locked_fr.page_id;
            for item in exceptions.iter() {
                if *item == page_id {
                    continue 'loop1;
                }
            }
            assert_eq!(
                0, locked_fr.pin_count,
                "err at frame {} with page_id {}: want 0 pin count, has {}",
                frame_id, page_id, locked_fr.pin_count
            );
        }
    }

    // caller must previously acquire latches of frames, not
    // it return the latches in batch
    pub fn batch_unpin_flush<'a>(
        &self,
        items: impl Iterator<Item = OwningHandle<Arc<Mutex<Frame>>, MutexGuard<'a, Frame>>>,
    ) -> Result<(), StrErr> {
        for item in items {
            BufferPool::unpin_flush_frame_locked(&self.bp, item.page_id, item, &self.dm)
                .expect("failed to unpin and flush page");
        }
        Ok(())
    }
    pub fn flush_locked<'a>(&self, item: &mut MutexGuard<'a, Frame>) -> Result<(), StrErr> {
        BufferPool::flush_frame_locked(&self.bp, item.page_id, item, &self.dm)
            .expect("failed to flush page");
        Ok(())
    }

    pub fn unpin_locked<'a>(
        &self,
        item: &mut MutexGuard<'a, Frame>,
        dirty: bool,
    ) -> Result<(), StrErr> {
        BufferPool::unpin_frame_locked(&self.bp, item.page_id, item, dirty)
            .expect("failed to flush page");
        Ok(())
    }

    /* pub fn unpin_page(&self, page_id: PageID, dirty: bool) -> Result<bool, StrErr> {
        BufferPool::unpin_page(&self.bp, page_id, dirty)
    } */
    /* pub fn flush_page(&self, page_id: PageID) -> Result<(), StrErr> {
        BufferPool::flush_page(&self.bp, page_id, &self.dm)
    } */
}

pub struct BufferPool<R: Replacer> {
    frames: RefCell<Vec<Arc<Mutex<Frame>>>>,
    page_table: RefCell<HashMap<i64, FrameID>>,
    size: usize,

    meta: RefCell<PoolMeta>,
    replacer: R,
    free_list: RefCell<VecDeque<FrameID>>,
}
struct PoolMeta {
    next_new: i64,
}

#[allow(dead_code)]
impl<R> BufferPool<R>
where
    R: Replacer,
{
    fn new(max_size: usize, r: R) -> Self {
        let mut frames = Vec::new();
        let mut free_list = VecDeque::new();
        for i in 0..max_size {
            frames.push(Arc::new(Mutex::new(Frame {
                page_id: INVALID_PAGE_ID,
                id: i,
                dirty: false,
                _1: [0; 7],
                pin_count: 0,
                raw_data: [0u8; PAGE_SIZE],
                #[cfg(feature = "testing")]
                last_borrower: String::new(),
            })));
            free_list.push_front(i);
        }
        BufferPool {
            frames: RefCell::new(frames),
            page_table: RefCell::new(HashMap::new()),
            free_list: RefCell::new(free_list),
            replacer: r,
            size: max_size,
            meta: RefCell::new(PoolMeta { next_new: 0 }),
        }
    }

    fn _allocate_page_id_locked(b: &MutexGuard<Self>) -> PageID {
        let new_page = b.meta.borrow_mut().next_new;
        b.meta.borrow_mut().next_new = new_page + 1;
        return new_page;
    }
    fn _prepare_new_frame_meta(
        b: &MutexGuard<BufferPool<R>>,
        locked_frame: &MutexGuard<Frame>,
        frame_id: FrameID,
        new_page_id: PageID,
    ) -> Result<(), StrErr> {
        let old_page_id = locked_frame.page_id;
        if locked_frame.page_id != INVALID_PAGE_ID {
            let some_frame = b.page_table.borrow_mut().remove(&old_page_id);
            match some_frame {
                Some(deleted_frame_id) => {
                    if deleted_frame_id != frame_id {
                        return Err(StrErr::new(
                            "frame_id mismach between pointing frame_id and recorded frame_id",
                        ));
                    }
                }
                None => {
                    return Err(StrErr::new(
                        "page table does not have page received from current frame id",
                    ));
                }
            };
        }
        let maybe_old_frame = b
            .page_table
            .borrow_mut()
            .insert(new_page_id, locked_frame.id);
        match maybe_old_frame {
            Some(_) => {
                return Err(StrErr::new(
                    "inserting new page id but return unexpect item from map",
                ));
            }
            None => {}
        };
        Ok(())
    }

    fn fetch_frame<'b>(
        b: &MutexGuard<BufferPool<R>>,
        free_frame: FrameID,
    ) -> Result<Arc<Mutex<Frame>>, StrErr> {
        let chosen_frame_p: &Arc<Mutex<Frame>>;
        unsafe {
            // TODO: it this safe
            chosen_frame_p =
                &mut *(&mut b.frames.borrow_mut()[free_frame] as *mut Arc<Mutex<Frame>>);
        }
        let chosen_frame = Arc::clone(chosen_frame_p);
        Ok(chosen_frame)
    }

    fn new_page(mu: &Mutex<BufferPool<R>>, dm: &DiskManager) -> Result<Arc<Mutex<Frame>>, StrErr> {
        let b = mu.lock();

        let (free_frame, victimed) = Self::_frame_from_freelist_or_replacer(&b)?;
        let chosen_frame = Self::fetch_frame(&b, free_frame)?;
        let mut locked_chosen_frame = chosen_frame.lock();
        let new_page_id = Self::_allocate_page_id_locked(&b);

        Self::_prepare_new_frame_meta(&b, &locked_chosen_frame, free_frame, new_page_id)?;

        drop(b);
        if victimed && locked_chosen_frame.dirty {
            dm.write_from_frame_to_file(
                locked_chosen_frame.page_id,
                &mut locked_chosen_frame.raw_data[..],
            )?;
        }

        locked_chosen_frame.assign_new(new_page_id)?;
        locked_chosen_frame.pin();
        drop(locked_chosen_frame);
        Ok(chosen_frame)
    }

    fn _check_page_available_in_buffer(
        b: &MutexGuard<BufferPool<R>>,
        page_id: PageID,
    ) -> Option<FrameID> {
        let page_table = &b.page_table.borrow();

        let maybe_frame = page_table.get(&page_id);
        match maybe_frame {
            Some(frame_id) => Some(*frame_id),
            None => None,
        }
    }

    fn _check_and_get_page_available_in_buffer(
        b: &MutexGuard<BufferPool<R>>,
        page_id: PageID,
    ) -> Result<Option<Arc<Mutex<Frame>>>, StrErr> {
        let page_table = &b.page_table.borrow();

        let maybe_frame = page_table.get(&page_id);
        match maybe_frame {
            Some(frame_id) => {
                let frame = Self::fetch_frame(&b, *frame_id)?;
                let mut locked_frame = frame.lock();
                locked_frame.pin();
                b.replacer.borrow().pin(*frame_id);
                drop(locked_frame);
                return Ok(Some(frame));
            }
            None => Ok(None),
        }
    }

    fn _frame_from_freelist_or_replacer(
        b: &MutexGuard<BufferPool<R>>,
    ) -> Result<(FrameID, bool), StrErr> {
        if b.free_list.borrow().len() != 0 {
            let maybe_frame = b.free_list.borrow_mut().pop_front();
            match maybe_frame {
                Some(popped) => return Ok((popped, false)),
                None => {
                    return Err(StrErr::new(
                        "free_list says it has len >0, popping return 0 item",
                    ));
                }
            }
        } else {
            match b.replacer.victim() {
                Some(frame_id) => return Ok((frame_id, true)),
                None => {
                    return Err(StrErr::new("oom"));
                }
            };
        }
    }

    fn fetch_page(
        mu: &Mutex<BufferPool<R>>,
        dm: &DiskManager,
        page_id: PageID,
    ) -> Result<Arc<Mutex<Frame>>, StrErr> {
        let b = mu.lock();

        match Self::_check_and_get_page_available_in_buffer(&b, page_id)? {
            Some(frame) => return Ok(frame),
            None => {}
        };

        // let mut victimed = false;
        let (free_frame, victimed) = Self::_frame_from_freelist_or_replacer(&b)?;

        let chosen_frame = Self::fetch_frame(&b, free_frame)?;
        let mut locked_chosen_frame = chosen_frame.lock();

        Self::_prepare_new_frame_meta(&b, &locked_chosen_frame, free_frame, page_id)?;

        drop(b);
        if victimed && locked_chosen_frame.dirty {
            dm.write_from_frame_to_file(
                locked_chosen_frame.page_id,
                &mut locked_chosen_frame.raw_data[..],
            )?;
        }
        locked_chosen_frame.assign_new(page_id)?;
        dm.read_into_frame(page_id, &mut locked_chosen_frame.raw_data[..])?;

        locked_chosen_frame.pin();
        drop(locked_chosen_frame);
        Ok(chosen_frame)
    }

    fn unpin_page(mu: &Mutex<BufferPool<R>>, page_id: PageID, dirty: bool) -> Result<bool, StrErr> {
        let b = mu.lock();
        match Self::_check_page_available_in_buffer(&b, page_id) {
            Some(frame_id) => {
                let frame = Self::fetch_frame(&b, frame_id)?;
                let mut locked_frame = frame.lock();
                locked_frame.pin_count -= 1;
                locked_frame.dirty = dirty;
                if locked_frame.pin_count == 0 {
                    b.replacer.borrow().unpin(frame_id);
                }
                Ok(true)
            }
            None => Ok(false),
        }
    }
    fn unpin_frame_locked<'op>(
        mu: &Mutex<BufferPool<R>>,
        page_id: PageID,
        locked: &mut MutexGuard<'op, Frame>,
        dirty: bool,
    ) -> Result<(), StrErr> {
        let b = mu.lock();
        locked.pin_count -= 1;
        locked.dirty = dirty;
        if locked.pin_count == 0 {
            b.replacer.borrow().unpin(locked.id);
        }
        Ok(())
    }

    fn flush_frame_locked<'op>(
        mu: &Mutex<BufferPool<R>>,
        page_id: PageID,
        locked: &mut MutexGuard<'op, Frame>,
        dm: &DiskManager,
    ) -> Result<(), StrErr> {
        dm.write_from_frame_to_file(page_id, &mut locked.raw_data[..])?;
        Ok(())
    }

    // TODO: mark this page_id inside some reuseable page allocator
    // + add deleted flag for future page fetch
    fn delete_page_locked<'op>(
        mu: &Mutex<BufferPool<R>>,
        mut locked: OwningHandle<Arc<Mutex<Frame>>, MutexGuard<'op, Frame>>,
    ) -> Result<(), StrErr> {
        let b = mu.lock();
        locked.pin_count -= 1;
        locked.dirty = false;
        if locked.pin_count == 0 {
            b.replacer.borrow().unpin(locked.id);
        } else {
            panic!("Some other thread is hold an arc to a deleted page, need to handle this logic here")
        }
        drop(b);
        Ok(())
    }

    fn unpin_flush_frame_locked<'op>(
        mu: &Mutex<BufferPool<R>>,
        page_id: PageID,
        mut locked: OwningHandle<Arc<Mutex<Frame>>, MutexGuard<'op, Frame>>,
        dm: &DiskManager,
    ) -> Result<(), StrErr> {
        let b = mu.lock();
        locked.pin_count -= 1;
        locked.dirty = false;
        if locked.pin_count == 0 {
            b.replacer.borrow().unpin(locked.id);
        }
        drop(b);
        dm.write_from_frame_to_file(page_id, &mut locked.raw_data[..])?;
        Ok(())
    }

    fn flush_page(
        mu: &Mutex<BufferPool<R>>,
        page_id: PageID,
        dm: &DiskManager,
    ) -> Result<(), StrErr> {
        let b = mu.lock();
        match Self::_check_page_available_in_buffer(&b, page_id) {
            Some(frame_id) => {
                let frame = Self::fetch_frame(&b, frame_id)?;
                drop(b);
                dm.write_from_frame_to_file(page_id, &mut frame.lock().raw_data[..])?;
            }
            None => {}
        }
        Ok(())
    }
}
const EMPTY_PAGE: [u8; PAGE_SIZE] = [0u8; PAGE_SIZE];

impl Frame {
    fn assign_new(&mut self, new_page_id: PageID) -> Result<(), StrErr> {
        self.page_id = new_page_id;
        self.dirty = false;

        copy(&mut &EMPTY_PAGE[..], &mut &mut self.raw_data[..])?;
        Ok(())
    }

    fn pin(&mut self) {
        self.pin_count += 1;
    }
}

pub type FrameID = usize;
pub type PageID = i64;

pub const INVALID_PAGE_ID: PageID = -1;

pub trait Replacer {
    fn victim(&self) -> Option<FrameID>;

    // frameID should not be victimized until unpin
    fn pin(&self, frame_id: FrameID);

    // allow frame to be victimizedable
    fn unpin(&self, frame_id: FrameID);

    // items that can be victimized
    fn size(&self) -> i64;
}

#[repr(C)]
pub struct Frame {
    id: FrameID,
    page_id: PageID,
    dirty: bool,
    _1: [u8; 7],
    raw_data: RawData,
    pin_count: i64,
    #[cfg(feature = "testing")]
    last_borrower: String,
}
impl Frame {
    pub fn get_page_id(&self) -> i64 {
        self.page_id
    }
    pub fn get_raw_data(&mut self) -> &mut RawData {
        &mut self.raw_data
    }

    pub fn new_from_raw(raw_data: [u8; PAGE_SIZE]) -> Frame {
        Frame {
            id: 0,
            page_id: 0,
            dirty: false,
            _1: [0; 7],
            pin_count: 0,
            raw_data,
            #[cfg(feature = "testing")]
            last_borrower: String::new(),
        }
    }
}

pub const PAGE_SIZE: usize = 4096;
pub type RawData = [u8; PAGE_SIZE];

pub struct DiskManager {
    f: Mutex<File>,
    page_size: usize,
}
#[derive(Debug)]
pub struct StrErr {
    root: String,
}
impl StrErr {
    pub fn new(st: &str) -> Self {
        StrErr {
            root: st.to_string(),
        }
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
    pub fn new_from_file(f: File, page_size: u64) -> Self {
        return DiskManager {
            f: Mutex::new(f),
            page_size: (page_size as usize),
        };
    }
    pub fn file_size(&self) -> Result<u64, StrErr> {
        let f = self.f.lock();
        let size = File::metadata(&*f)
            .expect("failed to get file metadata")
            .len();
        return Ok(size);
    }
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
        let mut f = self.f.lock();
        f.seek(SeekFrom::Start(page_id as u64 * self.page_size as u64))?;
        let read_bytes = f.read(&mut buf[..self.page_size])?;
        if read_bytes != self.page_size {
            return Err(StrErr::new("not enough byte read"));
        }
        Ok(())
    }

    pub fn write_from_frame_to_file(&self, page_id: PageID, buf: &mut [u8]) -> Result<(), StrErr> {
        if buf.len() != self.page_size {
            return Err(StrErr::new("frame has invalid length"));
        }
        let mut f = self.f.lock();
        f.seek(SeekFrom::Start(page_id as u64 * self.page_size as u64))?;
        let byte_written = f.write(buf)?;
        if byte_written != self.page_size {
            return Err(StrErr::new("invalid bytes written"));
        }
        File::sync_all(&mut f)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replacer::LRURepl;
    use rand::{thread_rng, RngCore};
    use std::io::{copy, Read};
    use tempfile::tempfile;

    #[test]
    fn test_sample() {
        let mut some_rng: Box<dyn RngCore> = Box::new(thread_rng());

        let pool_size = 10;
        let dm = DiskManager::new_from_file(tempfile().unwrap(), PAGE_SIZE as u64);
        let repl = LRURepl::new(pool_size);
        let bpm = BufferPool::new(10, repl);
        let mu = Mutex::new(bpm);

        let mut random_bin_data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        some_rng.read_exact(&mut random_bin_data[..]).unwrap();

        random_bin_data[PAGE_SIZE / 2] = '0' as u8;
        random_bin_data[PAGE_SIZE - 1] = '0' as u8;
        {
            let page0 = BufferPool::new_page(&mu, &dm).unwrap();
            assert_eq!(0, page0.lock().page_id);
            let mut w = &mut page0.lock().raw_data[..];
            let mut r = &random_bin_data[..];
            copy(&mut r, &mut w).unwrap();
        }

        for _ in 1..pool_size {
            match BufferPool::new_page(&mu, &dm) {
                Ok(_) => {}
                Err(some_err) => panic!("fetching page has err {:?}", some_err),
            };
        }
        for i in pool_size..pool_size * 2 {
            match BufferPool::new_page(&mu, &dm) {
                Ok(_) => {
                    panic!("not expect this call to return success")
                }
                Err(some_err) => assert_eq!("oom", some_err.root),
            };
        }

        for i in 0..5 {
            assert_eq!(true, BufferPool::unpin_page(&mu, i, true).unwrap());
            BufferPool::flush_page(&mu, i, &dm).unwrap();
        }

        for i in 0..4 {
            let some_page = BufferPool::new_page(&mu, &dm).unwrap();
        }
        let page0 = BufferPool::fetch_page(&mu, &dm, 0).unwrap();
        assert_eq!(&page0.lock().raw_data[..], &random_bin_data[..]);
        assert_eq!(true, BufferPool::unpin_page(&mu, 0, false).unwrap());

        match BufferPool::new_page(&mu, &dm) {
            Ok(_) => {}
            Err(some_err) => panic!("calling new page has err {:?}", some_err),
        }
        match BufferPool::fetch_page(&mu, &dm, 0) {
            Ok(_) => {
                panic!("not expect this call to return success")
            }
            Err(some_err) => assert_eq!("oom", some_err.root),
        }
    }

    #[test]
    fn test_binary() {
        let mut some_rng: Box<dyn RngCore> = Box::new(thread_rng());

        let pool_size = 10;
        let dm = DiskManager::new_from_file(tempfile().unwrap(), PAGE_SIZE as u64);
        let repl = LRURepl::new(pool_size);
        let bpm = BufferPool::new(10, repl);
        let mu = Mutex::new(bpm);

        let mut random_bin_data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        some_rng.read_exact(&mut random_bin_data[..]).unwrap();

        random_bin_data[PAGE_SIZE / 2] = '0' as u8;
        random_bin_data[PAGE_SIZE - 1] = '0' as u8;
        {
            let page0 = BufferPool::new_page(&mu, &dm).unwrap();
            assert_eq!(0, page0.lock().page_id);
            let mut w = &mut page0.lock().raw_data[..];
            let mut r = &random_bin_data[..];
            copy(&mut r, &mut w).unwrap();
        }

        for _ in 1..pool_size {
            match BufferPool::new_page(&mu, &dm) {
                Ok(_) => {}
                Err(some_err) => panic!("fetching page has err {:?}", some_err),
            };
        }
        for i in pool_size..pool_size * 2 {
            match BufferPool::new_page(&mu, &dm) {
                Ok(_) => {
                    panic!("not expect this call to return success")
                }
                Err(some_err) => assert_eq!("oom", some_err.root),
            };
        }

        for i in 0..5 {
            assert_eq!(true, BufferPool::unpin_page(&mu, i, true).unwrap());
            BufferPool::flush_page(&mu, i, &dm).unwrap();
        }

        for i in 0..5 {
            let some_page = BufferPool::new_page(&mu, &dm).unwrap();
            // this is important, after this line, the lock of the page is drop, so
            // that buffer pool can acquire the lock on the frame
            let page_id = some_page.lock().page_id;
            assert_eq!(true, BufferPool::unpin_page(&mu, page_id, false).unwrap());
        }
        let page0 = BufferPool::fetch_page(&mu, &dm, 0).unwrap();
        assert_eq!(&page0.lock().raw_data[..], &random_bin_data[..]);
        assert_eq!(true, BufferPool::unpin_page(&mu, 0, false).unwrap());
    }
}
