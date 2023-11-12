use crate::node::{MsgBuffer, Node, NodeType};
use crate::pool::NodeCache;
use crate::superblock;
use crate::types::MessageData;
use crate::types::{MessageType, OnDiskKey, SizedOnDisk};
use crate::{allocator::PageAllocator, node::ChildId, PAGESIZE};
use std::collections::VecDeque;
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
    fn copy_node(&mut self, old_id: &ChildId) -> ChildId {
        let new_page_id = self.superblock.alloc();
        let mut new_node = self.pool.get_mut(old_id).clone();
        new_node.dirt();
        // println!("New :{}, old: {}", old_id, new_page_id);
        self.pool.put(new_page_id, new_node);
        new_page_id
    }

    pub fn insert(&mut self, key: Vec<u8>, val: Vec<u8>) {
        // logging here
        let key = OnDiskKey::new(key);
        let msg_data = MessageData::new(MessageType::Insert, val);

        let mut buf = MsgBuffer::new();
        buf.insert(key, msg_data);
        let (mut child_id, mut p) = self.send_msgs_to_subtree(self.root, buf);
        assert!(p.is_empty());
        // while !res.1.is_empty() {
        // let node = self.pool.get_mut(self.root);
        // node.
        // }
        self.root = child_id;
        self.superblock.root = child_id;

        // let mut stack = vec![];
        // let mut current = self.root;
        // loop {
        //     let mut safe = self.superblock.safe_to_overwrite_in_place(current);
        //     if !safe {
        //         current = self.copy_node(&current);
        //         safe = true;
        //     };
        //     // All following code is safe
        //     let node = self.pool.get_mut(&current);
        //     if node.need_pre_split(&key, &msg_data) {
        //         let [right_sib_id, parent_id] = [self.superblock.alloc(), self.superblock.alloc()];
        //         let [right_sib, parent] = node.split(current, parent_id);
        //         self.pool.put(right_sib_id, right_sib);
        //         self.pool.put(parent_id, parent);
        //         current = parent_id;
        //     }
        //
        //     let node = self.pool.get_mut(&current);
        //     match &mut node.node_inner {
        //         NodeType::Leaf(leaf) => {
        //             leaf.insert(key, msg_data);
        //             if leaf.is_node_full() {
        //                 // split
        //                 let [right_sib_id, parent_id] = [self.superblock.alloc(), self.superblock.alloc()];
        //                 let [right_sib, parent] = node.split(current, parent_id);
        //                 self.pool.put(right_sib_id, right_sib);
        //                 self.pool.put(parent_id, parent);
        //                 current = parent_id;
        //             }
        //         }
        //         NodeType::Internel(internel) => {
        //             internel.insert_msg(key, msg_data);
        //             if internel.is_msg_buffer_full(key.size() + msg_data.size()) {
        //                 // flush msgs to sub tree, may split or merge
        //                 // flush msgs
        //             }
        //         }
        //         _ => unimplemented!()
        //         }
        //     }
    }

    /// Return new pivot, left, right child id if split
    /// Parent must not be full
    /// How to handle tons of msg? May need more than one split
    fn send_msgs_to_subtree(
        &mut self,
        mut current: ChildId,
        msgs: MsgBuffer,
    ) -> (ChildId, Vec<(OnDiskKey, ChildId)>) {
        let mut safe = self.superblock.safe_to_overwrite_in_place(current);
        if !safe {
            current = self.copy_node(&current);
            safe = true;
        };
        let mut node = self.pool.acquire(&current);
        let old_current = current;
        let pivots = match &mut node.node_inner {
            NodeType::Leaf(leaf) => {
                msgs.into_iter().for_each(|(key, msg)| leaf.apply(key, msg));
                // While loop
                let mut pivots = vec![];
                while leaf.is_node_full() {
                    let right_sib_id = self.superblock.alloc();
                    let (right_sib, median) = leaf.split();
                    self.pool.put(right_sib_id, right_sib);
                    pivots.push((median, right_sib_id));
                }
                pivots.reverse();
                pivots
                // if node.is_root() && !pivots.is_empty() {
                //     let parent = Node::new_internel_root(current, &pivots);
                //     let parent_id = self.superblock.alloc();
                //     node.unset_root();
                //     self.pool.put(parent_id, parent);
                //     (parent_id, vec![])
                // } else {
                //     (current, pivots)
                // }
            }
            NodeType::Internel(internal) => {
                // if node.need_pre_split(msgs.size()) {
                //     let right_sib_id = self.superblock.alloc();
                //     let (right_sib, median) = node.split();
                //     self.pool.put(right_sib_id, right_sib);
                //     if node.is_root() {
                //         let parent = Node::new_internel_root(median, current, right_sib_id);
                //         let parent_id = self.superblock.alloc();
                //         node.unset_root();
                //         self.pool.put(parent_id, parent);
                //         current = parent_id;
                //         return self.send_msgs_to_subtree(current, msgs);
                //     } else {
                //         return (current, Some((median, right_sib_id)))
                //     }
                // }
                internal.merge_buffers(msgs);
                while internal.is_msg_buffer_full(0) {
                    let c = internal.find_child_with_most_msgs();
                    let (child_id, new_pivots) =
                        self.send_msgs_to_subtree(c, internal.collect_msg_to_child(c));
                    internal.update_pivots(c, child_id, new_pivots)
                    // flush
                }
                // check merge
                // flush msg buffer if ...

                let mut pivots = vec![];
                while internal.is_pivots_full() {
                    let right_sib_id = self.superblock.alloc();
                    let (right_sib, median) = internal.split();
                    self.pool.put(right_sib_id, right_sib);
                    pivots.push((median, right_sib_id));
                }
                pivots.reverse();
                pivots
            }
            _ => unimplemented!(),
        };

        let res = if node.is_root() && !pivots.is_empty() {
            // Do it in while loop
            let parent = Node::new_internel_root(current, pivots);
            let parent_id = self.superblock.alloc();
            node.unset_root();
            self.pool.put(parent_id, parent);
            (parent_id, vec![])
        } else {
            (current, pivots)
        };
        self.pool.release(old_current, node);
        res
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        let key = OnDiskKey::new(key);
        let msg_data = MessageData::new(MessageType::Delete, vec![]);
        // logging here

        // Merging
    }

    pub fn upsert(&mut self, key: Vec<u8>, val: Vec<u8>) {
        let key = OnDiskKey::new(key);
        let msg_data = MessageData::new(MessageType::Upsert, val);

        // logging
    }

    pub fn get(&mut self, key: &[u8]) -> Option<&[u8]> {
        let key = OnDiskKey::new(key.to_vec());
        self.get_from_subtree(&key, self.root)
    }

    fn get_from_subtree<'a, 'b: 'a>(
        &'b mut self,
        key: &OnDiskKey,
        page: ChildId,
    ) -> Option<&'a [u8]> {
        let node = self.pool.get(&page);
        None
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
            let mut pool =
                NodeCache::new(&superblock.storage_filename, false, 10.try_into().unwrap());
            let root = superblock.root;
            assert!(pool.get(&root).is_root());
            Self {
                root,
                superblock,
                pool,
            }
        } else {
            Self::new(path)
        }
    }

    pub fn flush(&mut self) {
        self.pool.flush();
        self.superblock.flush_wal();
        self.superblock.flush_sb().unwrap();
    }

    fn print_tree(&mut self) {
        let mut q = VecDeque::new();
        let mut new_queue = VecDeque::new();
        q.push_back(self.root);
        loop {
            while let Some(n) = q.pop_front() {
                let node = self.pool.get(&n);
                let mut s = "".to_owned();
                match &node.node_inner {
                    NodeType::Internel(internel) => {
                        s.push_str("Internel ");
                        s.push_str(&format!(
                            "Msg buffer size: {}/{} ",
                            internel.msg_buffer.size(),
                            internel.get_msg_buffer_capacity()
                        ));
                        s.push_str(&format!(
                            "Pivots : {}/{}",
                            internel.get_pivots_capacity() - internel.get_pivots_avail(),
                            internel.get_pivots_capacity()
                        ));
                        internel
                            .pivot_map
                            .iter()
                            .for_each(|(k, c)| new_queue.push_back(*c));
                    }
                    NodeType::Leaf(leaf) => {
                        s.push_str("Leaf");
                        s.push_str(&format!(
                            " kv size: {}/{} ",
                            leaf.size(),
                            leaf.get_kv_capacity()
                        ));
                    }
                    _ => {}
                }
                print!("Node ID({}): {} | ", n, s);
            }
            println!();
            if new_queue.is_empty() {
                break;
            } else {
                core::mem::swap(&mut q, &mut new_queue);
            }
        }
    }
}

#[test]
fn test_btree() {
    use rand::prelude::*;
    use rand_chacha::ChaCha8Rng;
    use std::collections::HashMap;
    let mut rng = ChaCha8Rng::seed_from_u64(2);
    let mut betree = Betree::open("/tmp/test_betree");
    // betree.print_tree();
    // println!("Superblock root: {}", betree.superblock.last_flushed_root);
    let test_cap = 40000;
    let mut ref_map = HashMap::with_capacity(test_cap);
    for i in 0..test_cap {
        let k = vec![rng.gen(), rng.gen()];
        let v = vec![rng.gen(), rng.gen()];
        ref_map.insert(k.clone(), v.clone());
        betree.insert(k, v);
    }
    betree.print_tree();
    assert!(false);
}
