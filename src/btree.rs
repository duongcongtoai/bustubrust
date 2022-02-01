use crate::bpm::Frame;
use crate::bpm::INVALID_PAGE_ID;
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

trait DBType: Copy + Ord + Pod + Debug {}

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
struct BreadCrumb<'a, K: Pod, V: Pod> {
    // need this to handle lock/unlock
    origin: OwningHandle<Arc<Mutex<Frame>>, MutexGuard<'a, Frame>>,
    _mapped: NodePage<'a, K, V>,
    ref_idx: usize,
}

struct Access<'a, K: DBType, V: DBType> {
    bread_crumbs: Vec<BreadCrumb<'a, K, V>>,
    to_cleaned: Vec<i64>,
    to_flushed: Vec<i64>,
}
impl<'a, K: DBType, V: DBType> Access<'a, K, V> {
    fn pop_next(&mut self) -> Option<BreadCrumb<'a, K, V>> {
        let ret = self.bread_crumbs.pop()?;
        self.to_cleaned.push(ret.origin.get_page_id());
        return Some(ret);
    }
}
impl<K: DBType, V: DBType> Default for Access<'_, K, V> {
    fn default() -> Self {
        Access {
            bread_crumbs: vec![],
            to_cleaned: vec![],
            to_flushed: vec![],
        }
    }
}

#[allow(dead_code)]
impl<'a, R: Replacer, K: DBType, V: DBType> Tree<'a, R, K, V> {
    fn _get_page<'op>(&mut self, page_id: i64) -> Result<BreadCrumb<'op, K, V>, StrErr> {
        let bpm = self.bpm;
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

        let b = BreadCrumb {
            origin: guard,
            _mapped: node,
            ref_idx: 0,
        };
        Ok(b)
    }

    fn _get_root<'op>(&mut self) -> Result<BreadCrumb<'op, K, V>, StrErr> {
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
                idx_from_parent = branch.find_next_child(&bread_crumb._mapped.header, search_key);
                bread_crumb.ref_idx = idx_from_parent;
                let next = branch.children[idx_from_parent];
                if next != INVALID_PAGE_ID {
                    let next_node = self._get_root()?;
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

    fn insert(&mut self, key: K, val: V) -> Result<(), StrErr> {
        let mut tx = self._search_leaf(&key)?;
        let node = tx.pop_next().expect("want at least one breadcrumb item");
        let leaf = node._mapped.leaf();
        let idx = leaf.find_slot(&node._mapped.header, &key);
        let comp = Val { key, val };
        if leaf.data[idx] == comp {
            return Err(StrErr::new("duplicate key found"));
        }
        // leaf.data.insert()
        Ok(())
    }
    /*
        n := breadCrumb.node

        // normal insertion
        {
            idx, exact := t.leafNodeFindKeySlot(n, key)
            if exact {
                return fmt.Errorf("duplicate key found %v", key)
            }
            copy(n.datas[idx+1:n.size+1], n.datas[idx:n.size])
            n.datas[idx] = valT{
                val: keyT{main: int64(val)},
                key: key,
            }
            n.size++
        }

        if n.size < t._header.nodeSize {
            return nil
        }
        orphan, splitKey, err := t.splitLeafNode(&tx, n)
        if err != nil {
            return err
        }

        // retrieve currentParent from cursor latest stack
        // if currentParent ==nil, create new currentParent(in this case current leaf is also the root node)
        var currentParent *genericNode

        for len(tx.breadCrumbs) > 0 {
            curStack := tx.breadCrumbs[len(tx.breadCrumbs)-1]
            currentParent = curStack.node
            tx.breadCrumbs = tx.breadCrumbs[:len(tx.breadCrumbs)-1]

            idx, err := currentParent.findUniquePointerIdx(splitKey)
            if err != nil {
                return err
            }

            currentParent._insertPointerAtIdx(int64(idx), &orphanNode{
                key:        splitKey,
                rightChild: orphan,
            }, t._header.nodeSize)
            if currentParent.size < t._header.nodeSize {
                return nil
            }
            // this parent is also full

            newOrphan, newSplitKey, err := t.splitBranchNode(&tx, currentParent)
            if err != nil {
                return err
            }

            // let the next iteration handle this split with new parent propagated up the stack
            orphan = newOrphan
            splitKey = newSplitKey
        }
        // if reach this line, the higest level parent (root) has been recently split
        root, err := t.getRootNode()
        if err != nil {
            return err
        }

        newLevel := root.level + 1

        newRoot, err := t.newEmptyBranchNode()
        if err != nil {
            return err
        }
        tx.addUnpin(nodeID(newRoot.osPage.GetPageID()))
        newRoot.level = newLevel
        newRoot.children[0] = nodeID(root.osPage.GetPageID())
        newRoot._insertPointerAtIdx(0, &orphanNode{
            key:        splitKey,
            rightChild: orphan,
        }, t._header.nodeSize)
        t._header.rootPgid = nodeID(newRoot.osPage.GetPageID())

        // headerPage Updated, flush instead of unpin (header page is always pinned)
        tx.addFlush(0)
        return nil
    } */

    fn new(bpm: &'a BufferPoolManager<R>, node_size: i64) -> Result<Tree<'a, R, K, V>, StrErr> {
        match bpm.fetch_page(0) {
            Ok(header_frame) => {
                // header_frame.into_inner().get_raw_data()
                let mut locked = header_frame.lock();
                // let raw = header_frame.get_raw_data();
                let h = HeaderPage::cast(locked.get_raw_data());
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
                if h.flags & HEADER_FLAG_INIT == 0 {
                    h.flags ^= HEADER_FLAG_INIT;

                    h.node_size = node_size;
                    let root_page = bpm.new_page()?;
                    let mut locked_page = root_page.lock();
                    let page_id = locked_page.get_page_id();
                    // first time db is created, prepare an empty leaf-root node
                    let _: NodePage<K, V> =
                        NodePage::cast_leaf_from_blank(node_size, locked_page.get_raw_data());
                    h.root_id = page_id;
                    bpm.flush_page(0)?; // new root page
                    bpm.flush_page(page_id)?;
                    bpm.unpin_page(page_id, false)?;
                }

                h.flags ^= HEADER_FLAG_LOCKED;
                drop(locked);
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
                println!("dbug: {:?} vs {:?}", keys.len(), header.size);
                if header.size > keys.len() as i64 {
                    println!("here");
                }
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

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
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
    fn find_slot(&self, h: &PageHeader, search_key: &K) -> usize {
        return self.data[..h.size as usize].partition_point(|&x| x.key <= *search_key);
    }
}

impl<'a, K: DBType> BranchData<'a, K> {
    fn find_next_child(&self, h: &PageHeader, search_key: &K) -> usize {
        return self.keys[..h.size as usize].partition_point(|&x| x <= *search_key);
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
impl<'a, K: Sized + Pod, V: Sized + Pod> NodePage<'a, K, V> {
    fn branch(&self) -> &'a BranchData<K> {
        if let PageData::B(some_branch) = &self.data {
            &some_branch
        } else {
            panic!("want branch data")
        }
    }
    fn leaf(&self) -> &'a LeafData<K, V> {
        if let PageData::L(some_leaf) = &self.data {
            &some_leaf
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
    size: i64,
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
    fn cast<'a>(raw: &'a mut [u8]) -> &'a mut HeaderPage {
        try_from_bytes_mut::<HeaderPage>(&mut raw[..24]).unwrap()
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
    use bytemuck::Pod;
    use rand::{thread_rng, Rng, RngCore};
    use std::io::{Read, Seek, SeekFrom, Write};
    use tempfile::tempfile;

    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct KeyT {
        main: i64,
        sub: i64,
    }

    /* #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct ValT {
        key: KeyT,
        val: KeyT,
    }
    impl DBType for ValT {}

    unsafe impl Pod for ValT {}
    unsafe impl Zeroable for ValT {} */

    unsafe impl Pod for KeyT {}
    unsafe impl Zeroable for KeyT {}
    impl DBType for KeyT {}

    #[test]
    fn test_bin_search() {
        let mut keys = vec![];
        let mut children = vec![-1];
        for i in 0..10 {
            keys.push(KeyT { main: i, sub: 0 });
            children.push(i);
        }
        let branch = BranchData {
            keys: SliceVec::from_slice_len(&mut keys[..], 10),
            children: SliceVec::from_slice_len(&mut children[..], 11),
        };
        // vector from 0..9

        // (size of slice, search key, expect index returned)
        let suites = vec![(9, 0, 1), (9, -1, 0), (9, 9, 9)];
        for item in &suites {
            let header = PageHeader {
                is_deleted: false,
                is_leaf: false,
                _padding2: [0; 6],
                level: 0,
                size: item.0,
                next: 0,
            };

            let ret = branch.find_next_child(
                &header,
                &KeyT {
                    main: item.1,
                    sub: 0,
                },
            );
            assert_eq!(item.2, ret);
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
        for i in 0..node_size as usize {
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
