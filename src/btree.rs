use crate::node::Node;
use crate::pool::NodeCache;
use crate::superblock;
use crate::{allocator::PageAllocator, node::ChildId, PAGESIZE};
use std::path::Path;
use superblock::Superblock;

const BTREE_SPLIT_THRESHOLD: u64 = PAGESIZE / 2;

pub struct Betree {
    root: ChildId,
    // memtable: Memtable,
    pool: NodeCache,
    superblock: Superblock,
}

impl Betree {
    pub fn insert(&mut self, key: Vec<u8>, val: Vec<u8>) {}

    pub fn upsert(&mut self, key: Vec<u8>, val: Vec<u8>) {}

    pub fn get(&mut self, key: &[u8]) -> Vec<u8> {
        vec![]
    }

    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let mut superblock = Superblock::new(&path);
        let mut pool = NodeCache::new(&superblock.storage_filename, true, 10.try_into().unwrap());
        let root = Node::new_empty_leaf(true);
        let page_id = superblock.allocator.alloc();
        pool.put(page_id, root);
        pool.write_through(&page_id);
        superblock.set_root(page_id);
        superblock.flush_sb().unwrap();
        Self {
            root: page_id,
            superblock,
            pool,
        }
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        if Superblock::exists(&path) {
            let superblock = Superblock::open(path);
            let pool = NodeCache::new(&superblock.storage_filename, false, 10.try_into().unwrap());
            let root = superblock.root;
            Self {
                root,
                superblock,
                pool,
            }
        } else {
            Self::new(path)
        }
    }

    pub fn flush(&mut self) {}
}

#[test]
fn test_btree() {
    let _betree = Betree::open("/tmp/test_betree");
}
