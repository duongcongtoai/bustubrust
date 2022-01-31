use crate::bpm::Frame;
use crate::bpm::{BufferPoolManager, Replacer, StrErr};
use bytemuck::try_from_bytes_mut;
use bytemuck::{try_cast_slice_mut, Pod, Zeroable};
use iota::iota;
use owning_ref::OwningHandle;
use parking_lot::{Mutex, MutexGuard};
use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::Arc;

trait DBType: Copy + Ord + Pod {}

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
    ref_idx: i64,
}

struct Tx<'a, K: DBType, V: DBType> {
    bread_crumbs: Vec<BreadCrumb<'a, K, V>>,
    to_cleaned: Vec<i64>,
    to_flushed: Vec<i64>,
}
impl<K: DBType, V: DBType> Default for Tx<'_, K, V> {
    fn default() -> Self {
        Tx {
            bread_crumbs: vec![],
            to_cleaned: vec![],
            to_flushed: vec![],
        }
    }
}
/* struct PageLatch<'a, R: RawMutex> {
    origin: Arc<Mutex<Frame>>,
    locked: MutexGuard<'a, R, Frame>,
    _mapped: NodePage<'a, K, V>,
} */

#[allow(dead_code)]
impl<'a, R: Replacer, K: DBType, V: DBType> Tree<'a, R, K, V> {
    fn _get_root<'b>(&mut self) -> Result<BreadCrumb<'b, K, V>, StrErr> {
        let mut bpm = self.bpm;
        let root_frame = self.bpm.fetch_page(self.h.root_id)?;

        let mut guard = OwningHandle::new_with_fn(root_frame, |mutex: *const Mutex<Frame>| {
            let mutex: &Mutex<Frame> = unsafe { &*mutex };
            return mutex.lock();
        });
        let node: NodePage<'b, K, V>;
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
    fn _search_leaf<'b>(&mut self, search_key: &K) -> Result<Tx<'b, K, V>, StrErr> {
        let mut tx = Tx::default();
        let mut root = self._get_root()?;

        let cur_level = root._mapped.header.level;
        let mut node = root;
        let idx_from_parent = 0;
        while node._mapped.header.is_leaf {
            assert!(
                cur_level > 0,
                "reached level 0 node but still have not found leaf node"
            );
            node.ref_idx = idx_from_parent;
            tx.bread_crumbs.push(node);
            return Err(StrErr::new(""));
        }
        return Err(StrErr::new(""));

        /* _assert(len(c.breadCrumbs) == 0, "length of cursor is not cleaned up")
        root, err := t.getRootNode()
        if err != nil {
            return fmt.Errorf("failed to get root node: %v", err)
        }
        var curNode = root
        curLevel := root.level
        var pointerIdx int
        for !curNode.isLeafNode {
            _assert(curLevel > 0, "reached level 0 node but still have not found leaf node")
            c.breadCrumbs = append(c.breadCrumbs, breadCrumb{
                node: curNode,
                idx:  pointerIdx,
            })
            pointerIdx = curNode.branchNodeFindPointerIdx(searchKey)
            if curNode.children[pointerIdx] != invalidID {
                nextNodePageID := curNode.children[pointerIdx]

                if nextNodePageID == 0 {
                    nextNodePageID = 4
                }
                curNode, err = t.getGenericNode(nextNodePageID)
                if err != nil {
                    return err
                }
                curLevel--
                continue
            }
            panic(fmt.Sprintf("cannot find correct node for key %v", searchKey))
        }
        c.breadCrumbs = append(c.breadCrumbs, breadCrumb{
            node: curNode,
            idx:  pointerIdx,
        }) */
    }

    fn insert(&mut self, k: K, v: V) -> Result<(), StrErr> {
        let tx = self._search_leaf(&k);
        Ok(())
    }
    /* func (t *btreeCursor) insert(key keyT, val int64) error {
        tx := tx{}
        // cur.stack from root -> nearest parent
        err := tx.searchLeafNode(t, key)
        if err != nil {
            return err
        }
        defer tx.unpinPages(t.bpm)
        breadCrumb, ok := tx.popNext()
        if !ok {
            panic("not reached")
        }
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
        let (raw_header, next) = raw.split_at_mut(32);
        let header = try_from_bytes_mut::<PageHeader>(raw_header).unwrap();
        let page_data: PageData<'a, K, V>;
        match header.is_leaf {
            true => {
                let end = node_size as usize * size_of::<V>();
                let (raw_data, _) = next.split_at_mut(end);
                let leaf_data: &mut [V] = try_cast_slice_mut(raw_data).unwrap();
                page_data = PageData::L(LeafData { data: leaf_data });
            }
            false => {
                let keys_end = node_size as usize * size_of::<K>();
                let (raw_keys, next) = next.split_at_mut(keys_end);
                let keys: &mut [K] = try_cast_slice_mut(raw_keys).unwrap();
                let children_end = (node_size + 1) as usize * size_of::<i64>();
                let (raw_children, _) = next.split_at_mut(children_end);
                let children: &mut [i64] = try_cast_slice_mut(raw_children).unwrap();
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

        let end = node_size as usize * size_of::<V>();
        let (raw_data, _) = next.split_at_mut(end);
        let leaf_data: &mut [V] = try_cast_slice_mut(raw_data).unwrap();
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
        let children_end = (node_size + 1) as usize * size_of::<i64>();
        let (raw_children, _) = next.split_at_mut(children_end);
        let children: &mut [i64] = try_cast_slice_mut(raw_children).unwrap();
        page_data = PageData::B(BranchData { keys, children });
        let node = NodePage {
            data: page_data,
            header,
        };
        node
    }
}

// unsafe impl Zeroable for PageHeader {}

unsafe impl Pod for PageHeader {}
unsafe impl Zeroable for PageHeader {}

impl<'a, K: DBType> BranchData<'a, K> {
    fn find_pointer_idx(&self, search_key: K) -> Result<usize, StrErr> {
        match self.keys.binary_search(&search_key) {
            Ok(idx) => Ok(idx),
            Err(err_idx) => Err(StrErr::new(
                format!("data is not sorted at index: {:?}", err_idx).as_str(),
            )),
        }
    }
    /* func (n *genericNode) branchNodeFindPointerIdx(searchKey keyT) int {
        var (
            exactmatch bool
        )
        foundIdx := sort.Search(int(n.size), func(curIdx int) bool {
            curKey := n.keys[curIdx]
            comp := compareKey(curKey, searchKey)
            if comp == 0 {
                exactmatch = true
            }
            return comp == 0 || comp == 1
        })

        if exactmatch {
            foundIdx = foundIdx + 1
        }
        return foundIdx
    } */
}

struct NodePage<'a, K, V>
where
    K: Sized + Pod,
    V: Sized + Pod,
{
    header: &'a mut PageHeader,
    data: PageData<'a, K, V>,
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
    L(LeafData<'a, V>),
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
    keys: &'a mut [K],
    children: &'a mut [i64],
}

#[derive(Debug)]
struct LeafData<'a, V>
where
    V: Sized + Clone + Pod,
{
    data: &'a mut [V],
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

    #[derive(Copy, Clone, Debug, PartialEq)]
    struct KeyT {
        main: i64,
        sub: i64,
    }

    #[derive(Copy, Clone, Debug, PartialEq)]
    struct ValT {
        key: KeyT,
        val: KeyT,
    }
    unsafe impl Pod for ValT {}
    unsafe impl Zeroable for ValT {}

    unsafe impl Pod for KeyT {}
    unsafe impl Zeroable for KeyT {}

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
        let size = 9;
        let next = 7;
        let level = 10;
        let mut some_file = tempfile().unwrap();
        let mut fake_data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        let mut some_page: NodePage<KeyT, ValT> =
            NodePage::cast_branch_from_blank(node_size, &mut fake_data[..]);
        some_page.header.size = size;
        some_page.header.level = level;
        let branch_data: BranchData<_>;
        match some_page.data {
            PageData::B(branch) => {
                assert_eq!(node_size as usize + 1, branch.children.len());
                assert_eq!(node_size as usize, branch.keys.len());
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

            branch_data.keys[i] = key;
            let node_id = some_rng.gen();
            branch_data.children[i] = node_id;
            checked_data.push((key, node_id));
        }
        assert_eq!(PAGE_SIZE, some_file.write(&mut fake_data[..]).unwrap());
        let mut new_buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        some_file.seek(SeekFrom::Start(0)).unwrap();

        some_file.read_exact(&mut new_buf[..]).unwrap();
        let some_page2 = NodePage::<KeyT, ValT>::cast_generic(node_size, &mut new_buf[..]);
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
        let size = 9;
        let next = 7;
        let level = 10;
        let mut some_file = tempfile().unwrap();
        let mut fake_data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        let mut some_page: NodePage<KeyT, ValT> =
            NodePage::cast_leaf_from_blank(node_size, &mut fake_data[..]);
        some_page.header.size = size;
        some_page.header.next = next;
        some_page.header.level = level;
        let leaf_data: LeafData<_>;
        match some_page.data {
            PageData::L(leaf) => {
                assert_eq!(node_size as usize, leaf.data.len());
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
            let val = KeyT {
                main: some_rng.gen(),
                sub: some_rng.gen(),
            };
            leaf_data.data[i] = ValT { key, val };
            checked_data.push(ValT { key, val });
        }
        assert_eq!(PAGE_SIZE, some_file.write(&mut fake_data[..]).unwrap());
        some_file.flush().unwrap();
        let mut new_buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        some_file.seek(SeekFrom::Start(0)).unwrap();

        some_file.read_exact(&mut new_buf[..]).unwrap();
        let some_page2 = NodePage::<KeyT, ValT>::cast_generic(node_size, &mut new_buf[..]);
        some_page2.header.size = size;
        some_page2.header.next = next;
        let leaf_data2: LeafData<_>;
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
