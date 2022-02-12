use crate::bpm::Frame;
use crate::bpm::INVALID_PAGE_ID;
use crate::bpm::PAGE_SIZE;
use crate::bpm::{BufferPoolManager, Replacer, StrErr};
use bytemuck::try_from_bytes_mut;
use bytemuck::{try_cast_slice_mut, Pod, Zeroable};
use iota::iota;
use owning_ref::OwningHandle;
use parking_lot::{Mutex, MutexGuard};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::Arc;
use tinyvec::SliceVec;

trait DBType: Copy + Ord + Pod + Debug + Default {}

#[allow(dead_code)]
struct Tree<'a, R, K, V>
where
    R: Replacer,
    K: DBType,
    V: DBType,
{
    h: &'a mut HeaderPage,
    h_lock: Arc<Mutex<Frame>>,
    bpm: &'a BufferPoolManager<R>,
    _1: PhantomData<(K, V)>,
}
struct PageLatch<'a, K: Pod, V: Pod> {
    // need this to handle lock/unlock
    origin: OwningHandle<Arc<Mutex<Frame>>, MutexGuard<'a, Frame>>,
    _mapped: NodePage<'a, K, V>,
    ref_idx: usize,
}

struct Access<'a, K: DBType, V: DBType> {
    bread_crumbs: Vec<PageLatch<'a, K, V>>,
    to_clean: Vec<PageLatch<'a, K, V>>,
    flush_head: bool,
    temp: Option<PageLatch<'a, K, V>>,
    fetched_cousins: Vec<PageLatch<'a, K, V>>, // during borrowing, cousins
                                               //may be prefetched and reuse for merge operations
}

impl<'a, K: DBType, V: DBType> Access<'a, K, V> {
    fn pop_next(&mut self) -> Option<PageLatch<'a, K, V>> {
        let ret = self.bread_crumbs.pop()?;
        return Some(ret);
    }

    fn add_flush(&mut self, latch: PageLatch<'a, K, V>) {
        self.to_clean.push(latch);
    }

    fn cache_fetched_cousins(&mut self, cousin_latch: PageLatch<'a, K, V>) {
        self.fetched_cousins.push(cousin_latch);
    }

    fn find_fetched_cousin(&mut self, page_id: i64) -> Option<PageLatch<'a, K, V>> {
        let mut found_idx = -1;
        for (idx, cousin) in self.fetched_cousins.iter().enumerate() {
            if cousin.origin.get_page_id() == page_id {
                found_idx = idx as i64;
                break;
            }
        }
        if found_idx == -1 {
            panic!("not sure if this is reachable");
        }
        let cousin = self.fetched_cousins.remove(found_idx as usize);
        return Some(cousin);
    }
}
impl<K: DBType, V: DBType> Default for Access<'_, K, V> {
    fn default() -> Self {
        Access {
            bread_crumbs: vec![],
            to_clean: vec![],
            fetched_cousins: vec![],
            temp: None,
            flush_head: false,
        }
    }
}

#[allow(dead_code)]
impl<'a, R: Replacer, K: DBType, V: DBType> Tree<'a, R, K, V> {
    fn _get_page<'op>(&self, page_id: i64) -> Result<PageLatch<'op, K, V>, StrErr> {
        // let bpm = self.bpm;
        let root_frame = self.bpm.fetch_page(page_id)?;

        let mut guard = OwningHandle::new_with_fn(root_frame, |mutex: *const Mutex<Frame>| {
            let mutex: &Mutex<Frame> = unsafe { &*mutex };
            return mutex.lock();
        });
        let node: NodePage<'op, K, V>;
        unsafe {
            // this raw_data lives as long as the guard
            let raw_data = &mut *(guard.get_raw_data() as *mut [u8]);
            node = NodePage::cast_generic(self.h.node_size, raw_data);
        };

        let b = PageLatch {
            origin: guard,
            _mapped: node,
            ref_idx: 0,
        };
        Ok(b)
    }

    fn _get_root<'op>(&mut self) -> Result<PageLatch<'op, K, V>, StrErr> {
        return self._get_page(self.h.root_id);
    }

    fn _search_leaf<'op>(&mut self, search_key: &K) -> Result<Access<'op, K, V>, StrErr> {
        let mut acc = Access::default();
        let root = self._get_root()?;

        let mut cur_level = root._mapped.header.level;
        let mut bread_crumb = root;
        let mut idx_from_parent = 0;
        while !bread_crumb._mapped.header.is_leaf {
            assert!(
                cur_level > 0,
                "reached level 0 node but still have not found leaf node"
            );
            if let PageData::B(ref branch) = bread_crumb._mapped.data {
                idx_from_parent = branch.find_next_child(search_key);
                bread_crumb.ref_idx = idx_from_parent;
                let next = branch.children[idx_from_parent];
                if next != INVALID_PAGE_ID {
                    let next_node = self._get_page(next)?;
                    cur_level -= 1;
                    acc.bread_crumbs.push(bread_crumb);
                    bread_crumb = next_node;
                    continue;
                }
                panic!("cannot find correct node for key {:?}", *search_key);
            } else {
                return Err(StrErr::new("page data is not branch page"));
            }
        }
        bread_crumb.ref_idx = idx_from_parent;
        acc.bread_crumbs.push(bread_crumb);
        return Ok(acc);
    }
    fn _new_empty_branch<'op>(&self) -> Result<PageLatch<'op, K, V>, StrErr> {
        let frame_for_new_page = self.bpm.new_page().expect("unable to allocate new page");
        let mut guard =
            OwningHandle::new_with_fn(frame_for_new_page, |mutex: *const Mutex<Frame>| {
                let mutex: &Mutex<Frame> = unsafe { &*mutex };
                return mutex.lock();
            });
        let node: NodePage<'op, K, V>;
        unsafe {
            // this raw_data lives as long as the guard
            let raw_data = &mut *(guard.get_raw_data() as *mut [u8]);

            node = NodePage::cast_branch_from_blank(self.h.node_size, raw_data);
        };

        let b = PageLatch {
            origin: guard,
            _mapped: node,
            ref_idx: 0,
        };
        Ok(b)
    }

    fn _new_empty_leaf<'op>(&self) -> Result<PageLatch<'op, K, V>, StrErr> {
        let frame_for_new_page = self.bpm.new_page().expect("unable to allocate new page");
        let mut guard =
            OwningHandle::new_with_fn(frame_for_new_page, |mutex: *const Mutex<Frame>| {
                let mutex: &Mutex<Frame> = unsafe { &*mutex };
                return mutex.lock();
            });
        let node: NodePage<'op, K, V>;
        unsafe {
            // this raw_data lives as long as the guard
            let raw_data = &mut *(guard.get_raw_data() as *mut [u8]);

            node = NodePage::cast_leaf_from_blank(self.h.node_size, raw_data);
        };

        let b = PageLatch {
            origin: guard,
            _mapped: node,
            ref_idx: 0,
        };
        Ok(b)
    }
    fn _split_branch_node<'op>(
        &self,
        n: &mut NodePage<'op, K, V>,
    ) -> Result<(PageLatch<'op, K, V>, K), StrErr> {
        let mut new_right_node = self
            ._new_empty_branch()
            .expect("unable to create new blank branch");
        new_right_node._mapped.header.level = n.header.level;
        let partition_idx = self.h.node_size as usize / 2;
        let old_branch = n.data.branch();

        let split_key = old_branch.keys[partition_idx];
        let new_branch = &mut new_right_node._mapped.data.branch();
        new_branch
            .keys
            .extend_from_slice(&old_branch.keys[partition_idx + 1..]);
        new_branch
            .children
            .extend_from_slice(&old_branch.children[partition_idx + 1..]);
        for item in old_branch.keys[partition_idx..].iter_mut() {
            *item = K::default()
        }
        for item in old_branch.children[partition_idx + 1..].iter_mut() {
            *item = 0
        }

        old_branch.keys.truncate(partition_idx);
        old_branch.children.truncate(partition_idx + 1);

        // fix headers
        new_right_node._mapped.header.size = n.header.size - partition_idx as i64 - 1;
        n.header.size = partition_idx as i64;

        Ok((new_right_node, split_key))
    }

    fn _split_leaf_node<'op>(
        &self,
        n: &mut NodePage<'op, K, V>,
    ) -> Result<(PageLatch<'op, K, V>, K), StrErr> {
        let mut new_node = self
            ._new_empty_leaf()
            .expect("unable to create new blank leaf");
        let partition_idx = self.h.node_size as usize / 2;
        let old_leaf = n.data.leaf();

        let new_leaf = &mut new_node._mapped.data.leaf();
        new_leaf
            .data
            .extend_from_slice(&old_leaf.data[partition_idx..]);
        for item in old_leaf.data[partition_idx..].iter_mut() {
            *item = Val::default()
        }
        old_leaf.data.truncate(partition_idx);

        // fix headers
        new_node._mapped.header.size = n.header.size - partition_idx as i64;
        new_node._mapped.header.next = n.header.next;
        n.header.size = partition_idx as i64;
        n.header.next = new_node.origin.get_page_id();
        let split_key = new_leaf.data[0].key;

        Ok((new_node, split_key))
    }

    fn _insert_dirty<'op>(&mut self, key: K, val: V) -> Result<Access<'op, K, V>, StrErr> {
        let node_size = self.h.node_size;

        // traverse the tree to find slot for this key
        let mut acc = self._search_leaf(&key)?;
        let mut written_leaf_latch = acc.pop_next().expect("want at least one breadcrumb item");
        let mut leaf_header = &mut written_leaf_latch._mapped.header;
        let leaf_page = &mut written_leaf_latch._mapped.data;

        let leaf_data = leaf_page.leaf();
        {
            let comp = Val { key, val };
            let idx = leaf_data.find_slot(&comp)?;
            leaf_data.data.insert(idx, comp);
            leaf_header.size += 1;
        }
        // valid size
        if leaf_header.size < node_size {
            acc.add_flush(written_leaf_latch);
            return Ok(acc);
        }
        let (orphan, split_key) = self
            ._split_leaf_node(&mut written_leaf_latch._mapped)
            .expect("unable to split node");
        let mut orphan_id = orphan.origin.get_page_id();
        let mut split_key = split_key;
        {
            if acc.bread_crumbs.len() == 0 {
                let mut new_root = self
                    ._new_empty_branch()
                    .expect("unable to create new blank branch");
                let new_level = orphan._mapped.header.level + 1;
                new_root._mapped.header.level = new_level;
                let new_root_branch = new_root._mapped.data.branch();
                let current_root = self.h.root_id;
                new_root._mapped.header.size += 1;
                new_root_branch.children.push(current_root);
                new_root_branch.children.push(orphan_id);
                new_root_branch.keys.push(split_key);
                self.h.root_id = new_root.origin.get_page_id();
                acc.flush_head = true;

                acc.add_flush(written_leaf_latch);
                acc.add_flush(orphan);
                acc.add_flush(new_root);
                return Ok(acc);
            }
        }
        acc.add_flush(written_leaf_latch);
        acc.add_flush(orphan);

        loop {
            let mut current_parent_latch = acc.pop_next().expect("not expect return empty item");
            let current_parent = current_parent_latch._mapped.data.branch();
            let idx = current_parent.find_slot(&split_key)?;
            current_parent.children.insert(idx + 1, orphan_id);
            current_parent.keys.insert(idx, split_key);
            current_parent_latch._mapped.header.size += 1;
            if current_parent_latch._mapped.header.size < node_size {
                acc.add_flush(current_parent_latch);
                return Ok(acc);
            }

            let (new_orphan, new_slit_key) = self
                ._split_branch_node(&mut current_parent_latch._mapped)
                .expect("unable to split branch node");
            orphan_id = new_orphan.origin.get_page_id();

            split_key = new_slit_key;
            if acc.bread_crumbs.len() == 0 {
                let mut new_root = self
                    ._new_empty_branch()
                    .expect("unable to create new blank branch");
                // acc.add_flush(new_root.origin.get_page_id());
                let new_level = new_orphan._mapped.header.level + 1;
                new_root._mapped.header.level = new_level;
                new_root._mapped.header.size += 1;
                let new_root_branch = new_root._mapped.data.branch();
                let current_root = self.h.root_id;
                new_root_branch.children.push(current_root);
                new_root_branch.children.push(orphan_id);
                new_root_branch.keys.push(split_key);
                self.h.root_id = new_root.origin.get_page_id();
                acc.flush_head = true;

                acc.add_flush(new_root);
                acc.add_flush(new_orphan);
                acc.add_flush(current_parent_latch);
                break;
            }

            acc.add_flush(new_orphan);
            acc.add_flush(current_parent_latch);
        }
        return Ok(acc);
    }
    fn _return_access_to_bpm<'op>(&self, mut acc: Access<'op, K, V>) -> Result<(), StrErr> {
        let bpm = self.bpm;
        let h_lock = self.h_lock.clone();

        let untouched = acc.bread_crumbs.into_iter().map(|x| x.origin);

        if let Some(latch) = acc.temp {
            acc.to_clean.push(latch);
        }

        let flush_unpins = acc.to_clean.into_iter().map(|x| x.origin);
        let total_flushed = untouched
            .chain(flush_unpins)
            .chain(acc.fetched_cousins.into_iter().map(|x| x.origin));

        bpm.batch_unpin_flush(total_flushed)
            .expect("failed flushing in batch");
        if acc.flush_head {
            let mut locked = h_lock.lock();
            bpm.flush_locked(&mut locked)
                .expect("failed to flush header page");
        }

        Ok(())
    }

    // Borrowing from cousins may cause deadlock, because there can be and scan leaf operation
    // going on.
    // TODO: make _get_page acquire a lock with retry mode
    // Reference (Improved Latch Crabbing Protocol): https://15445.courses.cs.cmu.edu/fall2021/notes/08-indexconcurrency.pdf
    fn _try_borrow_cousins_key<'op>(
        &self,
        acc: &mut Access<'op, K, V>,
        parent: &mut NodePage<'op, K, V>,
        current_node: &mut NodePage<'op, K, V>,
        current_node_idx: usize,
    ) -> Result<bool, StrErr> {
        let is_leaf = current_node.header.is_leaf;
        let parent_branch = parent.data.branch();

        // prefer borrowing for right cousin first, according to this
        // https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html
        if current_node_idx < parent_branch.children.len() - 1 {
            let right_cousin_id = parent_branch.children[current_node_idx + 1];
            let mut right_cousin_latch = self._get_page(right_cousin_id)?;
            if right_cousin_latch._mapped.header.size > self.h.node_size / 2 {
                //leaf
                if is_leaf {
                    let right_cousin = right_cousin_latch._mapped.data.leaf();
                    let right_first = right_cousin.data[0];
                    let current_leaf = current_node.data.leaf();

                    current_leaf.data.push(right_first);
                    current_node.header.size += 1;

                    right_cousin.data.remove(0);
                    right_cousin_latch._mapped.header.size -= 1;

                    let new_key_for_parent = right_cousin.data[0].key;
                    parent_branch.keys[current_node_idx] = new_key_for_parent;
                    acc.cache_fetched_cousins(right_cousin_latch);
                    return Ok(true);
                }

                //branch
                let split_key = parent_branch.keys[current_node_idx];
                let right_cousin = right_cousin_latch._mapped.data.branch();
                let current_branch = current_node.data.branch();
                current_branch.keys.push(split_key);
                current_branch.children.push(right_cousin.children[0]);
                current_node.header.size += 1;

                let new_split_key = right_cousin.keys[0];
                right_cousin.keys.remove(0);
                right_cousin.children.remove(0);
                right_cousin_latch._mapped.header.size -= 1;

                parent_branch.keys[current_node_idx] = new_split_key;

                acc.cache_fetched_cousins(right_cousin_latch);
                return Ok(true);
            }
            acc.cache_fetched_cousins(right_cousin_latch);
            return Ok(false);
        }

        // if current node has a right cousin
        if current_node_idx > 0 {
            let left_cousin_id = parent_branch.children[current_node_idx - 1];
            let mut left_cousin_latch = self._get_page(left_cousin_id)?;
            if left_cousin_latch._mapped.header.size > self.h.node_size / 2 {
                //leaf
                if is_leaf {
                    let left_cousin = left_cousin_latch._mapped.data.leaf();
                    let left_last_key = left_cousin.data.pop().unwrap();
                    let current_leaf = current_node.data.leaf();
                    current_leaf
                        .data
                        .resize(current_leaf.data.len() + 1, left_last_key);
                    current_leaf.data.rotate_right(1);

                    current_node.header.size += 1;
                    left_cousin_latch._mapped.header.size -= 1;

                    let new_key_for_parent = current_leaf.data[0].key;
                    parent_branch.keys[current_node_idx - 1] = new_key_for_parent;
                    acc.cache_fetched_cousins(left_cousin_latch);
                    return Ok(true);
                }

                //branch
                let split_key = parent_branch.keys[current_node_idx - 1];
                let left_cousin = left_cousin_latch._mapped.data.branch();
                let current_branch = current_node.data.branch();
                current_branch
                    .keys
                    .resize(current_branch.keys.len() + 1, split_key);
                current_branch.keys.rotate_right(1);
                let left_cousin_last_child = left_cousin.children.pop().unwrap();
                let left_cousin_last_key = left_cousin.keys.pop().unwrap();
                current_branch
                    .children
                    .resize(current_branch.children.len(), left_cousin_last_child);

                current_node.header.size += 1;
                left_cousin_latch._mapped.header.size -= 1;

                parent_branch.keys[current_node_idx - 1] = left_cousin_last_key;
                acc.cache_fetched_cousins(left_cousin_latch);
                return Ok(true);
            }

            acc.cache_fetched_cousins(left_cousin_latch);
            return Ok(false);
        }

        panic!("not reach");
    }

    // TODO: make this generic for leaf node and branch node
    fn _merge_node_right_to_left<'op>(
        &self,
        acc: &mut Access<'op, K, V>,
        parent: &mut NodePage<'op, K, V>,
        idx_of_left: usize,
        left_node: &mut NodePage<'op, K, V>,
        right_node_latch: PageLatch<'op, K, V>,
    ) -> Result<(), StrErr> {
        let is_leaf = left_node.header.is_leaf;
        if !is_leaf {
            let mut right_node = right_node_latch._mapped;
            let parent_branch = parent.data.branch();
            let left_branch = left_node.data.branch();
            let right_branch = right_node.data.branch();
            left_branch
                .children
                .extend_from_slice(&right_branch.children[..]);
            let old_split_key = parent_branch.keys[idx_of_left];
            left_branch.keys.push(old_split_key);
            left_branch.keys.extend_from_slice(&right_branch.keys[..]);
            left_node.header.size += right_branch.keys.len() as i64 + 1;

            parent_branch.children.remove(idx_of_left + 1);
            parent_branch.keys.remove(idx_of_left);
            parent.header.size -= 1;
            self.bpm.delete_page_locked(right_node_latch.origin)?;
            return Ok(());
        }
        let mut right_node = right_node_latch._mapped;
        let parent_branch = parent.data.branch();
        let left_leaf = left_node.data.leaf();
        let right_leaf = right_node.data.leaf();
        left_leaf.data.extend_from_slice(&right_leaf.data[..]);
        left_node.header.size += right_leaf.data.len() as i64;
        parent_branch.keys.remove(idx_of_left);
        parent_branch.children.remove(idx_of_left + 1);
        parent.header.size -= 1;

        left_node.header.next = right_node.header.next;
        self.bpm.delete_page_locked(right_node_latch.origin)?;
        Ok(())
    }

    fn _delete_dirty<'op>(&mut self, key: K) -> Result<Access<'op, K, V>, StrErr> {
        let merge_threshold = self.h.node_size / 2;
        let mut acc = self
            ._search_leaf(&key)
            .expect("cannot find leaf node io insert to key");
        let mut node_page_latch = acc.pop_next().expect("not expect to return empty");
        let node_page = &mut node_page_latch._mapped;
        let leaf = node_page.data.leaf();
        leaf.delete_with_key(&key)?;
        node_page.header.size -= 1;

        let mut maybe_new_root = INVALID_PAGE_ID;
        // let current_size = node_page.header.size;

        // acc.add_flush(node_page_latch);
        // while let Some(node_page_latch) = self._bubleup(&mut acc,)
        acc.temp = Some(node_page_latch);
        while let Some(mut parent_latch) = acc.pop_next() {
            let mut node_page_latch = acc.temp.take().unwrap();
            let node_page = &mut node_page_latch._mapped;
            if node_page.header.size >= merge_threshold {
                acc.add_flush(node_page_latch);
                acc.add_flush(parent_latch);
                return Ok(acc);
            }
            let current_node_id = node_page_latch.origin.get_page_id();
            let ref_idx = parent_latch.ref_idx;
            if self._try_borrow_cousins_key(
                &mut acc,
                &mut parent_latch._mapped,
                node_page,
                ref_idx,
            )? {
                // CLEAN UP and return
                acc.add_flush(node_page_latch);
                acc.add_flush(parent_latch);
                return Ok(acc);
            }
            let parent_branch = parent_latch._mapped.data.branch();
            if ref_idx > 0 {
                let left_cousin_id = parent_branch.children[ref_idx - 1];

                // let mut left_page_latch = self._get_page(left_cousin_id)?;
                let mut left_page_latch = acc.find_fetched_cousin(left_cousin_id).unwrap();
                self._merge_node_right_to_left(
                    &mut acc,
                    &mut parent_latch._mapped,
                    ref_idx - 1,
                    &mut left_page_latch._mapped,
                    node_page_latch,
                )?;

                maybe_new_root = left_cousin_id;
                acc.temp = Some(parent_latch);
                // acc.add_flush(left_page_latch);
                acc.add_flush(left_page_latch);
            } else if ref_idx < parent_latch._mapped.header.size as usize {
                let right_cousin_id = parent_branch.children[ref_idx + 1];

                // this caching is to avoid deadlock (with current impl)
                // because in previous step we always fetch cousin  (with raii lock) first to find if its keys are borrowable
                let right_page_latch = acc.find_fetched_cousin(right_cousin_id).unwrap();
                // let right_page_latch = self._get_page(right_cousin_id)?;
                self._merge_node_right_to_left(
                    &mut acc,
                    &mut parent_latch._mapped,
                    ref_idx,
                    node_page,
                    right_page_latch,
                )?;

                maybe_new_root = current_node_id;
                acc.temp = Some(parent_latch);
                // acc.add_flush(node_page_latch);
                acc.add_flush(node_page_latch);
            } else {
                panic!(
                    "should not reach here, ref_idx {:?} and parent size: {:?}",
                    ref_idx, parent_latch._mapped.header.size
                );
            }
        }
        let previous_parent = acc.temp.take().unwrap();
        // hard to imagine
        // this is a case when a root node used to be a parent, but after merge, it has no key
        // so the previous merged node is the new root node
        if previous_parent._mapped.header.size == 0 {
            if maybe_new_root != INVALID_PAGE_ID {
                self.h.root_id = maybe_new_root;
                acc.flush_head = true;

                acc.add_flush(previous_parent);
                return Ok(acc);
            }
        }
        acc.add_flush(previous_parent);
        Ok(acc)
    }

    fn insert(&mut self, key: K, val: V) -> Result<(), StrErr> {
        let acc = self._insert_dirty(key, val).expect("failed to insert");
        let ret = self._return_access_to_bpm(acc);
        #[cfg(feature = "testing")]
        self.bpm.assert_clean_frame(&[0]);
        ret
    }

    fn delete(&mut self, key: K) -> Result<(), StrErr> {
        let acc = self._delete_dirty(key).expect("failed to insert");
        let ret = self._return_access_to_bpm(acc);
        #[cfg(feature = "testing")]
        self.bpm.assert_clean_frame(&[0]);
        ret
    }

    fn new(bpm: &'a BufferPoolManager<R>, node_size: i64) -> Result<Tree<'a, R, K, V>, StrErr> {
        if bpm.dm.file_size()? < PAGE_SIZE as u64 {
            let header_frame = bpm.new_page().expect("failed to create new page");
            let mut locked_header = header_frame.lock();
            if locked_header.get_page_id() != 0 {
                return Err(StrErr::new("newly created header page has id not equal 0"));
            }
            bpm.flush_locked(&mut locked_header)?;
        }
        match bpm.fetch_page(0) {
            Ok(header_frame) => {
                // header_frame.into_inner().get_raw_data()
                let mut locked_header = header_frame.lock();
                // let raw = header_frame.get_raw_data();
                let h = HeaderPage::cast(locked_header.get_raw_data());
                let long_lived_header: &'a mut HeaderPage;

                // this is safe, as long as we make sure no other components can call
                // unpin(page_0), which will make the frame containing header page
                // be recycled to contains data from other data page
                unsafe {
                    long_lived_header = &mut *(h as *mut HeaderPage);
                }
                if h.flags & HEADER_FLAG_LOCKED != 0 {
                    return Err(StrErr::new("page file has been locked by other process"));
                }

                h.flags ^= HEADER_FLAG_LOCKED;
                if h.flags & HEADER_FLAG_INIT == 0 {
                    h.flags ^= HEADER_FLAG_INIT;

                    h.node_size = node_size;
                    let root_page = bpm.new_page().expect("unable to allocate new page");
                    let mut locked_root_page = root_page.lock();
                    let page_id = locked_root_page.get_page_id();
                    // first time db is created, prepare an empty leaf-root node
                    let _: NodePage<K, V> =
                        NodePage::cast_leaf_from_blank(node_size, locked_root_page.get_raw_data());
                    h.root_id = page_id;
                    bpm.flush_locked(&mut locked_header)
                        .expect("can't flush page 0"); // new root page
                    bpm.flush_locked(&mut locked_root_page)
                        .expect(format!("cant flush page {:?}", page_id).as_str());
                    bpm.unpin_locked(&mut locked_root_page, false)
                        .expect(format!("cant unpin page {:?}", page_id).as_str());
                }

                drop(locked_header);
                Ok(Tree {
                    h: long_lived_header,
                    h_lock: header_frame,
                    bpm,
                    _1: PhantomData,
                })
            }
            Err(some_err) => Err(StrErr::new(format!("todo: {:?}", some_err).as_str())),
        }
    }
}

#[allow(dead_code)]
impl<'a, K, V> NodePage<'a, K, V>
where
    K: Pod + Sized,
    V: Pod + Sized,
{
    fn cast_generic(node_size: i64, raw: &'a mut [u8]) -> NodePage<'a, K, V> {
        let (raw_header, next) = raw.split_at_mut(size_of::<PageHeader>());
        let header = try_from_bytes_mut::<PageHeader>(raw_header).unwrap();
        let page_data: PageData<'a, K, V>;
        match header.is_leaf {
            true => {
                let end = node_size as usize * size_of::<Val<K, V>>();
                let (raw_data, _) = next.split_at_mut(end);
                let leaf_data: &mut [Val<K, V>] = try_cast_slice_mut(raw_data).unwrap();
                let leaf_data = SliceVec::from_slice_len(leaf_data, header.size as usize);
                page_data = PageData::L(LeafData { data: leaf_data });
            }
            false => {
                let keys_end = node_size as usize * size_of::<K>();
                let (raw_keys, next) = next.split_at_mut(keys_end);
                let keys: &mut [K] = try_cast_slice_mut(raw_keys).unwrap();
                let keys = SliceVec::from_slice_len(keys, header.size as usize);

                let children_end = (node_size + 1) as usize * size_of::<i64>();
                let (raw_children, _) = next.split_at_mut(children_end);
                let children: &mut [i64] = try_cast_slice_mut(raw_children).unwrap();
                let children = SliceVec::from_slice_len(children, header.size as usize + 1);
                page_data = PageData::B(BranchData { keys, children });
            }
        }
        let node = NodePage {
            data: page_data,
            header,
        };
        node
    }

    fn cast_leaf_from_blank(node_size: i64, raw: &'a mut [u8]) -> NodePage<'a, K, V> {
        let (raw_header, next) = raw.split_at_mut(32);
        let header = try_from_bytes_mut::<PageHeader>(raw_header).unwrap();
        let page_data: PageData<'a, K, V>;
        header.is_leaf = true;
        header.next = INVALID_PAGE_ID;

        let end = node_size as usize * size_of::<Val<K, V>>();
        let (raw_data, _) = next.split_at_mut(end);
        let leaf_data: &mut [Val<K, V>] = try_cast_slice_mut(raw_data).unwrap();
        let leaf_data = SliceVec::from_slice_len(leaf_data, 0);
        page_data = PageData::L(LeafData { data: leaf_data });

        let node = NodePage {
            data: page_data,
            header,
        };
        node
    }

    fn cast_branch_from_blank(node_size: i64, raw: &'a mut [u8]) -> NodePage<'a, K, V> {
        let (raw_header, next) = raw.split_at_mut(32);
        let header = try_from_bytes_mut::<PageHeader>(raw_header).unwrap();
        let page_data: PageData<'a, K, V>;
        header.is_leaf = false;

        let keys_end = node_size as usize * size_of::<K>();
        let (raw_keys, next) = next.split_at_mut(keys_end);
        let keys: &mut [K] = try_cast_slice_mut(raw_keys).unwrap();
        let keys = SliceVec::from_slice_len(keys, 0);
        let children_end = (node_size + 1) as usize * size_of::<i64>();
        let (raw_children, _) = next.split_at_mut(children_end);
        let children: &mut [i64] = try_cast_slice_mut(raw_children).unwrap();
        let children = SliceVec::from_slice_len(children, 0);
        page_data = PageData::B(BranchData { keys, children });
        let node = NodePage {
            data: page_data,
            header,
        };
        node
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
struct Val<K, V> {
    key: K,
    val: V,
}

// unsafe impl Zeroable for PageHeader {}
unsafe impl<K: Pod, V: Pod> Pod for Val<K, V> {}
unsafe impl<K: Pod, V: Pod> Zeroable for Val<K, V> {}

unsafe impl Pod for PageHeader {}
unsafe impl Zeroable for PageHeader {}

impl<'a, K: DBType, V: DBType> LeafData<'a, K, V> {
    fn delete_with_key(&mut self, key: &K) -> Result<(), StrErr> {
        let idx = self.data.binary_search_by(|&x| x.key.cmp(key)).expect(
            format!(
                "failed to binary search at index {:?} with key {:?}",
                self.data, key,
            )
            .as_str(),
        );
        self.data.remove(idx);
        Ok(())
    }

    fn find_slot(&self, data_key: &Val<K, V>) -> Result<usize, StrErr> {
        let idx = self.data.partition_point(|&x| x.key <= data_key.key);
        if idx < self.data.len() && self.data[idx] == *data_key {
            return Err(StrErr::new("duplicate key found"));
        }
        return Ok(idx);
    }
}

impl<'a, K: DBType> BranchData<'a, K> {
    fn find_next_child(&self, search_key: &K) -> usize {
        return self.keys.partition_point(|&x| x <= *search_key);
    }
    fn find_slot(&self, data_key: &K) -> Result<usize, StrErr> {
        let idx = self.keys.partition_point(|&x| x <= *data_key);
        if idx < self.keys.len() && self.keys[idx] == *data_key {
            return Err(StrErr::new("duplicate key found"));
        }
        return Ok(idx);
    }
}

struct NodePage<'a, K, V>
where
    K: Sized + Pod,
    V: Sized + Pod,
{
    header: &'a mut PageHeader,
    data: PageData<'a, K, V>,
}
impl<'a, K: Sized + Pod, V: Sized + Pod> PageData<'a, K, V> {
    fn branch(&mut self) -> &mut BranchData<'a, K> {
        if let PageData::B(some_branch) = self {
            some_branch
        } else {
            panic!("want branch data")
        }
    }
    fn leaf(&mut self) -> &mut LeafData<'a, K, V> {
        if let PageData::L(some_leaf) = self {
            some_leaf
        } else {
            panic!("want leaf data")
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct PageHeader {
    is_deleted: bool,
    is_leaf: bool,
    _padding2: [u8; 6],
    level: i64,
    size: i64, // size of keys(for branch) or data (for leaf)
    next: i64,
}

enum PageData<'a, K, V>
where
    K: Sized + Pod,
    V: Sized + Pod,
{
    B(BranchData<'a, K>),
    L(LeafData<'a, K, V>),
}

unsafe impl Pod for HeaderPage {}
unsafe impl Zeroable for HeaderPage {}

#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq)]
struct HeaderPage {
    flags: i64,
    root_id: i64,
    node_size: i64,
}
/* unsafe impl Pod for HeaderPage {}
unsafe impl Zeroable for HeaderPage {} */

impl HeaderPage {
    fn cast(raw: &mut [u8]) -> &mut HeaderPage {
        try_from_bytes_mut::<HeaderPage>(&mut raw[..size_of::<HeaderPage>()]).unwrap()
    }
}

struct BranchData<'a, K>
where
    K: Sized + Pod,
{
    // keys: &'a mut [K],
    keys: SliceVec<'a, K>,
    children: SliceVec<'a, i64>,
    // children: &'a mut [i64],
}

#[derive(Debug)]
struct LeafData<'a, K, V>
where
    V: Sized + Clone + Pod,
    K: Sized + Clone + Pod,
{
    // data: &'a mut [Val<K, V>],
    data: SliceVec<'a, Val<K, V>>,
}

iota! {
    const HEADER_FLAG_INIT: i64 = 1 << iota;
    , HEADER_FLAG_LOCKED
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::bpm::PAGE_SIZE;
    use crate::{bpm::DiskManager, replacer::LRURepl};
    use bytemuck::Pod;
    // use core::fmt::Formatter;
    use rand::{seq, thread_rng, Rng, RngCore};
    use std::fmt::Formatter;
    use std::io::{Read, Seek, SeekFrom, Write};
    use tempfile::tempfile;

    #[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
    struct KeyT {
        main: i64,
        sub: i64,
    }
    impl Debug for KeyT {
        fn fmt(&self, fmt: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
            fmt.write_str(format!("{}-{}", self.main, self.sub).as_str())
        }
    }
    impl Into<KeyT> for i64 {
        fn into(self) -> KeyT {
            KeyT { main: self, sub: 0 }
        }
    }
    pub trait Into2<T>: Sized {
        fn into2(self) -> T;
    }

    impl<T: Into<KeyT>> Into2<Vec<KeyT>> for Vec<T> {
        fn into2(self) -> Vec<KeyT> {
            self.into_iter().map(|k| k.into()).collect()
        }
    }

    /* impl From<i64> for KeyT {
        fn from(i: i64) -> Self {
            KeyT { main: i, sub: 0 }
        }
    } */

    unsafe impl Pod for KeyT {}
    unsafe impl Zeroable for KeyT {}
    impl DBType for KeyT {}
    fn make_tree_key(input: &[i64]) -> Vec<KeyT> {
        let mut ret = vec![];
        for i in input {
            ret.push(KeyT { main: *i, sub: 0 });
        }
        ret
    }
    fn make_tree_val(input: &[i64]) -> Vec<Val<KeyT, KeyT>> {
        let mut ret = vec![];
        for item in input {
            let temp = KeyT {
                main: *item,
                sub: 0,
            };
            ret.push(Val {
                key: temp,
                val: temp,
            });
        }
        ret
    }
    fn sequential_until(last: i64) -> Vec<KeyT> {
        let mut ret = vec![];
        for i in 1..=last {
            ret.push(KeyT { main: i, sub: 0 });
        }
        ret
    }
    fn inverted_sequential_until(last: i64) -> Vec<KeyT> {
        let mut ret = vec![];
        for i in (1..=last).rev() {
            ret.push(KeyT { main: i, sub: 0 });
        }
        ret
    }

    #[test]
    fn test_delete() {
        _check_deadlock();
        let max_size = 10;
        struct Testcase {
            insertions: Vec<KeyT>,
            deletions: Vec<KeyT>,
            root_keys: Vec<KeyT>,
            leaf_vals: Vec<&'static [i64]>,
            node_size: i64,
        }
        let tcases: Vec<Testcase> = vec![
            Testcase {
                node_size: 3,
                insertions: sequential_until(5),
                deletions: vec![2].into2(),
                root_keys: make_tree_key(&[3, 4]),
                leaf_vals: vec![&[1], &[3], &[4, 5]],
            },
            Testcase {
                node_size: 3,
                insertions: sequential_until(3),
                deletions: vec![2, 1].into2(),
                root_keys: vec![3].into2(),
                leaf_vals: vec![&[3]],
            },
            Testcase {
                node_size: 3,
                insertions: sequential_until(3),
                deletions: vec![1].into2(),
                root_keys: vec![3].into2(),
                leaf_vals: vec![&[2], &[3]],
            },
            Testcase {
                node_size: 3,
                insertions: inverted_sequential_until(10),
                deletions: vec![10, 9, 8].into2(),
                root_keys: vec![5].into2(),
                leaf_vals: vec![&[1, 2], &[3, 4], &[5, 6], &[7]],
            },
            Testcase {
                node_size: 3,
                insertions: sequential_until(8),
                deletions: vec![4].into2(),
                root_keys: vec![3, 6].into2(),
                leaf_vals: vec![&[1], &[2], &[3], &[5], &[6], &[7, 8]],
            },
            Testcase {
                node_size: 3,
                insertions: sequential_until(6),
                deletions: vec![2].into2(),
                root_keys: vec![4].into2(),
                leaf_vals: vec![&[1], &[3], &[4], &[5, 6]],
            },
            Testcase {
                node_size: 3,
                insertions: sequential_until(5),
                deletions: vec![5, 4, 3].into2(),
                root_keys: vec![2].into2(),
                leaf_vals: vec![&[1], &[2]],
            },
            Testcase {
                node_size: 3,
                insertions: sequential_until(5),
                deletions: vec![5, 4, 3, 2].into2(),
                root_keys: vec![1].into2(),
                leaf_vals: vec![&[1]],
            },
        ];
        for case in tcases {
            let some_file = tempfile().unwrap();
            let repl = LRURepl::new(max_size);
            let dm = DiskManager::new_from_file(some_file, PAGE_SIZE as u64);
            let bpm = BufferPoolManager::new(max_size, repl, dm);

            let mut some_tree: Tree<_, KeyT, KeyT> =
                Tree::new(&bpm, case.node_size).expect("can't create new tree");

            for insertion in case.insertions {
                some_tree
                    .insert(insertion, insertion)
                    .expect(format!("failed to insert item {:?}", insertion).as_str());
            }
            for deletion in case.deletions {
                some_tree
                    .delete(deletion)
                    .expect(format!("failed to delete key {:?}", deletion).as_str());
            }
            #[cfg(feature = "testing")]
            some_tree.bpm.assert_clean_frame(&[0]);

            let left_most = KeyT { main: -1, sub: 0 };
            let mut acc = some_tree
                ._search_leaf(&left_most)
                .expect("unable to search left most leaf");
            let left_most_node = acc.pop_next().expect("not expect returning none");
            let mut cur_page_id = left_most_node.origin.get_page_id();
            acc.add_flush(left_most_node);
            some_tree
                ._return_access_to_bpm(acc)
                .expect("err during flushing access obj");

            let mut root_latch = some_tree._get_root().expect("failed to get root");
            match root_latch._mapped.header.is_leaf {
                true => {
                    let root_leaf = root_latch._mapped.data.leaf();
                    assert!(
                        case.root_keys
                            .iter()
                            .zip(root_leaf.data.iter())
                            .all(|(a, b)| *a == b.key),
                        "root keys not match: expect {:?}, has {:?}",
                        case.root_keys,
                        root_leaf.data
                    );
                }
                false => {
                    let root_branch = root_latch._mapped.data.branch();
                    assert!(
                        case.root_keys
                            .iter()
                            .zip(root_branch.keys.iter())
                            .all(|(a, b)| a == b),
                        "root keys not match: expect {:?}, has {:?}",
                        case.root_keys,
                        root_branch.keys
                    );
                }
            }

            /* let root_branch = root_latch._mapped.data.branch();
            assert!(
                case.root_keys
                    .iter()
                    .zip(root_branch.keys.iter())
                    .all(|(a, b)| a == b),
                "root keys not match: expect {:?}, has {:?}",
                case.root_keys,
                root_branch.keys
            ); */
            let st: Vec<OwningHandle<Arc<Mutex<Frame>>, MutexGuard<Frame>>> =
                vec![root_latch].into_iter().map(|l| l.origin).collect();
            some_tree
                .bpm
                .batch_unpin_flush(st.into_iter())
                .expect("failed to batch flush root page");
            // assert.Equal(t, tc.rootKeys, root.keys[:root.size])

            for (idx, raw_node_val) in case.leaf_vals.iter().enumerate() {
                let typed_node_val = make_tree_val(*raw_node_val);
                let mut page_latch = some_tree
                    ._get_page(cur_page_id)
                    .expect(format!("failed to get page {:?}", cur_page_id).as_str());
                let real_node_val = &page_latch._mapped.data.leaf().data;
                assert!(
                    typed_node_val
                        .iter()
                        .zip(real_node_val.iter())
                        .all(|(a, b)| a == b),
                    "case {:?} failed keys slices are not equal expect {:?} vs real {:?}",
                    idx,
                    typed_node_val,
                    real_node_val,
                );
                cur_page_id = page_latch._mapped.header.next;
            }
        }
    }

    fn _check_deadlock() {
        #[cfg(feature = "testing")]
        {
            // only for #[cfg]
            use parking_lot::deadlock;
            use std::thread;
            use std::time::Duration;

            // Create a background thread which checks for deadlocks every 10s
            thread::spawn(move || loop {
                thread::sleep(Duration::from_secs(2));
                let deadlocks = deadlock::check_deadlock();
                if deadlocks.is_empty() {
                    continue;
                }

                println!("{} deadlocks detected", deadlocks.len());
                for (i, threads) in deadlocks.iter().enumerate() {
                    println!("Deadlock #{}", i);
                    for t in threads {
                        println!("Thread Id {:#?}", t.thread_id());
                        println!("{:#?}", t.backtrace());
                    }
                }
            });
        }
    }

    #[test]
    fn test_insert() {
        let max_size = 10;
        struct Testcase {
            insertions: Vec<KeyT>,
            root_keys: Vec<KeyT>,
            leaf_vals: Vec<&'static [i64]>,
            node_size: i64,
        }
        let tcases: Vec<Testcase> = vec![
            Testcase {
                node_size: 3,
                insertions: sequential_until(4),
                root_keys: make_tree_key(&[2, 3]),
                leaf_vals: vec![&[1], &[2], &[3, 4]],
            },
            Testcase {
                node_size: 3,
                insertions: sequential_until(3),
                root_keys: make_tree_key(&[2]),
                leaf_vals: vec![&[1], &[2, 3]],
            },
            Testcase {
                node_size: 3,
                insertions: inverted_sequential_until(10),
                root_keys: make_tree_key(&[7]),
                leaf_vals: vec![&[1, 2], &[3, 4], &[5, 6], &[7, 8], &[9, 10]],
            },
            Testcase {
                node_size: 3,
                insertions: sequential_until(6),
                root_keys: vec![3].into2(),
                leaf_vals: vec![&[1], &[2], &[3], &[4], &[5, 6]],
            },
            Testcase {
                node_size: 4,
                insertions: make_tree_key(&[1, 3, 5, 9, 10]),
                root_keys: vec![5].into2(),
                leaf_vals: vec![&[1, 3], &[5, 9, 10]],
            },
            Testcase {
                node_size: 7,
                insertions: sequential_until(13),
                root_keys: vec![4, 7, 10].into2(),
                leaf_vals: vec![&[1, 2, 3], &[4, 5, 6], &[7, 8, 9], &[10, 11, 12, 13]],
            },
        ];
        for case in tcases {
            let some_file = tempfile().unwrap();
            let repl = LRURepl::new(max_size);
            let dm = DiskManager::new_from_file(some_file, PAGE_SIZE as u64);
            let bpm = BufferPoolManager::new(max_size, repl, dm);

            let mut some_tree: Tree<_, KeyT, KeyT> =
                Tree::new(&bpm, case.node_size).expect("can't create new tree");

            for insertion in case.insertions {
                some_tree
                    .insert(insertion, insertion)
                    .expect(format!("failed to insert item {:?}", insertion).as_str());
            }

            #[cfg(feature = "testing")]
            some_tree.bpm.assert_clean_frame(&[0]);

            let left_most = KeyT { main: -1, sub: 0 };
            let mut acc = some_tree
                ._search_leaf(&left_most)
                .expect("unable to search left most leaf");
            let left_most_node = acc.pop_next().expect("not expect returning none");
            let mut cur_page_id = left_most_node.origin.get_page_id();
            acc.add_flush(left_most_node);
            some_tree
                ._return_access_to_bpm(acc)
                .expect("err during flushing access obj");

            let mut root_latch = some_tree._get_root().expect("failed to get root");
            let root_branch = root_latch._mapped.data.branch();
            assert!(
                case.root_keys
                    .iter()
                    .zip(root_branch.keys.iter())
                    .all(|(a, b)| a == b),
                "root keys not match expect {:?} really {:?}",
                case.root_keys,
                root_branch.keys
            );
            let st: Vec<OwningHandle<Arc<Mutex<Frame>>, MutexGuard<Frame>>> =
                vec![root_latch].into_iter().map(|l| l.origin).collect();
            some_tree
                .bpm
                .batch_unpin_flush(st.into_iter())
                .expect("failed to batch flush root page");
            // assert.Equal(t, tc.rootKeys, root.keys[:root.size])

            for (idx, raw_node_val) in case.leaf_vals.iter().enumerate() {
                let typed_node_val = make_tree_val(*raw_node_val);
                let mut page_latch = some_tree
                    ._get_page(cur_page_id)
                    .expect(format!("failed to get page {:?}", cur_page_id).as_str());
                let real_node_val = &page_latch._mapped.data.leaf().data;
                assert!(
                    typed_node_val
                        .iter()
                        .zip(real_node_val.iter())
                        .all(|(a, b)| a == b),
                    "case {:?} failed keys slices are not equal expect {:?} vs real {:?}",
                    idx,
                    typed_node_val,
                    real_node_val,
                );
                cur_page_id = page_latch._mapped.header.next;
            }
        }
    }

    #[test]
    fn test_bin_search() {
        let mut keys = vec![];
        let mut children = vec![-1];
        for i in 0..10 {
            keys.push(KeyT { main: i, sub: 0 });
            children.push(i);
        }
        #[derive(Debug)]
        struct Suite {
            key: Vec<KeyT>,
            children: Vec<i64>,
            search_key: i64,
            expect_index: usize,
        }
        let mut test_case = vec![
            Suite {
                key: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9].into2(),
                children: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                search_key: 0,
                expect_index: 1,
            },
            Suite {
                key: vec![2].into2(),
                children: vec![1, 2],
                search_key: 4,
                expect_index: 1,
            },
        ];

        // vector from 0..9

        // (size of slice, search key, expect index returned)
        // let suites = vec![(9, 0, 1), (9, -1, 0), (9, 9, 9)];
        for case in &mut test_case {
            let keys = &mut case.key;
            let length = keys.len();
            let children = &mut case.children;
            let branch = BranchData {
                keys: SliceVec::from_slice_len(&mut keys[..], length),
                children: SliceVec::from_slice_len(&mut children[..], length + 1),
            };
            /* let header = PageHeader {
                is_deleted: false,
                is_leaf: false,
                _padding2: [0; 6],
                level: 0,
                size: length as i64,
                next: 0,
            }; */

            let ret = branch.find_next_child(&KeyT {
                main: case.search_key,
                sub: 0,
            });
            assert_eq!(case.expect_index, ret, "failed at item {:?}", case);
        }
    }

    #[test]
    fn test_cast_header() {
        let root_page_id = 7;
        let flags = HEADER_FLAG_INIT;
        let node_size = 9;

        let mut some_file = tempfile().unwrap();
        let mut fake_data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        let mut h = HeaderPage::cast(&mut fake_data[..]);
        h.node_size = node_size;
        h.flags = flags;
        h.root_id = root_page_id;
        some_file.write_all(&mut fake_data[..]).unwrap();
        some_file.flush().unwrap();
        some_file.seek(SeekFrom::Start(0)).unwrap();

        let mut new_buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        some_file.read_exact(&mut new_buf[..]).unwrap();
        let h2 = HeaderPage::cast(&mut new_buf[..]);
        assert_eq!(node_size, h2.node_size);
        assert_eq!(flags, h2.flags);
        assert_eq!(root_page_id, h2.root_id);
    }

    #[test]
    fn test_cast_branch() {
        let mut some_rng: Box<dyn RngCore> = Box::new(thread_rng());
        let node_size = 7;
        let size = 7;
        let next = 7;
        let level = 10;
        let mut some_file = tempfile().unwrap();
        let mut fake_data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        let mut some_page: NodePage<KeyT, KeyT> =
            NodePage::cast_branch_from_blank(node_size, &mut fake_data[..]);
        some_page.header.size = size;
        some_page.header.level = level;
        let mut branch_data: BranchData<_>;
        match some_page.data {
            PageData::B(branch) => {
                assert_eq!(node_size as usize + 1, branch.children.capacity());
                assert_eq!(node_size as usize, branch.keys.capacity());
                branch_data = branch;
            }
            PageData::L { .. } => panic!("not expect to return leaf variant"),
        };

        let mut checked_data = Vec::new(); // clone for further check
        for i in 0..node_size as usize {
            let key = KeyT {
                main: some_rng.gen(),
                sub: some_rng.gen(),
            };

            branch_data.keys.push(key);
            let node_id = some_rng.gen();
            branch_data.children.push(node_id);
            checked_data.push((key, node_id));
        }
        assert_eq!(PAGE_SIZE, some_file.write(&mut fake_data[..]).unwrap());
        let mut new_buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        some_file.seek(SeekFrom::Start(0)).unwrap();

        some_file.read_exact(&mut new_buf[..]).unwrap();
        let some_page2 = NodePage::<KeyT, KeyT>::cast_generic(node_size, &mut new_buf[..]);
        some_page2.header.size = size;
        some_page2.header.next = next;
        let branch_data2: BranchData<_>;
        match some_page2.data {
            PageData::B(branch) => {
                assert_eq!(node_size as usize + 1, branch.children.len());
                assert_eq!(node_size as usize, branch.keys.len());
                branch_data2 = branch;
            }
            PageData::L(_) => panic!("not expect to return leaf variant"),
        };
        assert_eq!(false, some_page2.header.is_leaf);
        assert_eq!(level, some_page2.header.level);

        assert!(
            checked_data
                .iter()
                .zip(branch_data2.keys.iter())
                .all(|(a, b)| a.0 == *b),
            "Keys slices are not equal"
        );
        assert!(
            checked_data
                .iter()
                .zip(branch_data2.children.iter())
                .all(|(a, b)| a.1 == *b),
            "Children slices are not equal"
        );
    }

    #[test]
    fn test_cast_leaf() {
        let mut some_rng: Box<dyn RngCore> = Box::new(thread_rng());
        let node_size = 7;
        let size = 7;
        let next = 7;
        let level = 10;
        let mut some_file = tempfile().unwrap();
        let mut fake_data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        let mut some_page: NodePage<KeyT, KeyT> =
            NodePage::cast_leaf_from_blank(node_size, &mut fake_data[..]);
        some_page.header.size = size;
        some_page.header.next = next;
        some_page.header.level = level;
        let mut leaf_data: LeafData<_, _>;
        match some_page.data {
            PageData::L(leaf) => {
                assert_eq!(node_size as usize, leaf.data.capacity());
                leaf_data = leaf;
            }
            PageData::B { .. } => panic!("not expect to return branch variant"),
        };

        let mut checked_data = Vec::new(); // clone for further check
        for _ in 0..node_size as usize {
            let key = KeyT {
                main: some_rng.gen(),
                sub: some_rng.gen(),
            };
            let val = Val { key, val: key };
            leaf_data.data.push(val);
            checked_data.push(val);
        }
        assert_eq!(PAGE_SIZE, some_file.write(&mut fake_data[..]).unwrap());
        some_file.flush().unwrap();
        let mut new_buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        some_file.seek(SeekFrom::Start(0)).unwrap();

        some_file.read_exact(&mut new_buf[..]).unwrap();
        let some_page2 = NodePage::<KeyT, KeyT>::cast_generic(node_size, &mut new_buf[..]);
        some_page2.header.size = size;
        some_page2.header.next = next;
        let leaf_data2: LeafData<_, _>;
        match some_page2.data {
            PageData::L(leaf) => {
                assert_eq!(node_size as usize, leaf.data.len());
                leaf_data2 = leaf;
            }
            PageData::B { .. } => panic!("not expect to return branch variant"),
        };
        assert_eq!(true, some_page2.header.is_leaf);
        assert_eq!(level, some_page2.header.level);

        assert!(
            checked_data
                .iter()
                .zip(leaf_data2.data.iter())
                .all(|(a, b)| *a == *b),
            "Arrays are not equal"
        );
    }
}
