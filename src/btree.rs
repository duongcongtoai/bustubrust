use crate::bpm::{BufferPoolManager, Frame, Replacer};
use bytemuck::try_from_bytes_mut;
use bytemuck::{cast, cast_ref, try_cast_slice_mut, try_from_bytes, Pod, Zeroable};
use parking_lot::{Mutex, MutexGuard};
// use std::slice::split_at_mut;
use std::{borrow::Borrow, mem::size_of, sync::Arc};

struct Tree<'a, R>
where
    R: Replacer,
{
    h: Option<HeaderPage>,
    bpm: &'a BufferPoolManager<R>,
}

impl<'a, R> Tree<'a, R>
where
    R: Replacer,
{
    fn new(bpm: &'a BufferPoolManager<R>, node_size: i64) -> Tree<'a, R> {
        match bpm.fetch_page(0) {
            Ok(header_frame) => {
                let locked = header_frame.lock();
            }
            Err(some_err) => {
                panic!(some_err);
            }
        }
        Tree { h: None, bpm }
    }
}

/* struct BorrowedFrame<'a, K, V>
where
    K: Pod + Sized,
    V: Pod + Sized,
{
    node: NodePage<'a, K, V>,
} */

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

unsafe impl Zeroable for PageHeader {}
unsafe impl Pod for PageHeader {}

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

#[repr(C)]
struct HeaderPage {
    flags: i64,
    root_id: i64,
    node_size: i64,
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

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::{bpm::PAGE_SIZE, btree::Frame};
    use bytemuck::Pod;
    // use parking_lot::Mutex;
    use rand::{thread_rng, Rng, RngCore};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::mem::align_of;
    // use std::sync::Arc;
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
    fn do_something() {
        println!("align of header {:?}", align_of::<PageHeader>());
        println!("size of header {:?}", size_of::<PageHeader>());
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
    /*
        assert.NoError(t, os.WriteFile(testFile, somePage, os.ModePerm))
        defer os.Remove(testFile)
        file, err := os.OpenFile(testFile, os.O_RDONLY, os.ModePerm)
        assert.NoError(t, err)
        newBuf := make([]byte, buff.PageSize)
        n, err := file.Read(newBuf)
        assert.NoError(t, err)
        assert.Equal(t, buff.PageSize, n)

        h2 := castGenericNode(10, newMockPage(newBuf))
        assert.Equal(t, h.size, h2.size)
        assert.Equal(t, h.keys, h2.keys)
        assert.Equal(t, h.children, h2.children)
        assert.False(t, h2.isLeafNode)
    } */

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
