use crate::memtable::Memtable;
use crate::pool::NodeCache;
use crate::{node::ChildId, types::*, PAGESIZE};
use std::collections::BTreeMap;

const BTREE_SPLIT_THRESHOLD: u64 = PAGESIZE / 2;

pub struct Betree {
    root: ChildId,
    memtable: Memtable,
    pool: NodeCache,
}

impl Betree {
    pub fn insert(&mut self, key: Vec<u8>, val: Vec<u8>) {}

    pub fn upsert(&mut self, key: Vec<u8>, val: Vec<u8>) {}

    pub fn get(&mut self, key: &[u8]) -> Vec<u8> {
        vec![]
    }
}
