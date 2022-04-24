use crate::bpm::FrameID;
use crate::bpm::Replacer;
use lru::LruCache;
use std::cell::RefCell;

pub struct LRURepl {
    internal: RefCell<LruCache<FrameID, ()>>,
}

impl LRURepl {
    pub fn new(cap: usize) -> Self {
        LRURepl {
            internal: RefCell::new(LruCache::new(cap)),
        }
    }
}

impl Replacer for LRURepl {
    fn victim(&self) -> Option<FrameID> {
        match self.internal.borrow_mut().pop_lru() {
            Some((frame_id, _)) => Some(frame_id),
            None => None,
        }
    }
    fn size(&self) -> i64 {
        self.internal.borrow().len() as i64
    }
    fn unpin(&self, frame_id: FrameID) {
        if !self.internal.borrow().contains(&frame_id) {
            self.internal.borrow_mut().put(frame_id, ());
        }
    }
    fn pin(&self, frame_id: FrameID) {
        self.internal.borrow_mut().pop(&frame_id);
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replacer() {
        let r = LRURepl::new(7);
        r.unpin(1);
        r.unpin(2);
        r.unpin(3);
        r.unpin(4);
        r.unpin(5);
        r.unpin(6);
        r.unpin(1);
        assert_eq!(6, r.size());
        let ret = r.victim();
        assert_eq!(ret, Some(1));
        let ret = r.victim();
        assert_eq!(ret, Some(2));
        let ret = r.victim();
        assert_eq!(ret, Some(3));
        r.pin(3);
        r.pin(4);

        assert_eq!(2, r.size());

        r.unpin(4);

        let ret = r.victim();
        assert_eq!(ret, Some(5));
        let ret = r.victim();
        assert_eq!(ret, Some(6));
        let ret = r.victim();
        assert_eq!(ret, Some(4));
    }
}
