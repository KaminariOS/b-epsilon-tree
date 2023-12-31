use crate::node::{InternalNode, MsgBuffer, Node, NodeType, MAX_KEY_SIZE};
use crate::pool::NodeCache;
use crate::superblock;
use crate::types::MessageData;
use crate::types::{MessageType, OnDiskKey, SizedOnDisk};
use crate::CFG;
use crate::{allocator::PageAllocator, node::ChildId};
use std::collections::{HashSet, VecDeque};
use std::path::Path;
use superblock::Superblock;

// const POOLSIZE: usize = 34000 / 1000;

pub struct Betree {
    root: ChildId,
    // memtable: Memtable,
    pool: NodeCache,
    superblock: Superblock,
}

impl Betree {
    fn copy_node(&mut self, old_id: &ChildId) -> ChildId {
        let new_page_id = self.superblock.alloc();
        let mut new_node = self.pool.get(old_id).clone();
        new_node.dirt();
        // println!("New :{}, old: {}", old_id, new_page_id);
        self.pool.put(new_page_id, new_node);
        new_page_id
    }

    pub fn insert(&mut self, key: Vec<u8>, val: Vec<u8>) {
        // logging here

        let key = OnDiskKey::new(key);
        assert!(key.size() <= MAX_KEY_SIZE);
        let msg_data = MessageData::new(MessageType::Insert, val);

        let mut buf = MsgBuffer::new();
        buf.insert(key, msg_data);
        let (child_id, p) = self.send_msgs_to_subtree(self.root, buf);
        debug_assert!(p.is_none());
        // while !res.1.is_empty() {
        // let node = self.pool.get_mut(self.root);
        // node.
        // }
        self.root = child_id;
        self.superblock.root = child_id;
        // self.pool.flush();
    }

    /// Return new pivot, left, right child id if split
    /// Parent must not be full
    /// How to handle tons of msg? May need more than one split
    fn send_msgs_to_subtree(
        &mut self,
        mut current: ChildId,
        msgs: MsgBuffer,
    ) -> (ChildId, Option<(OnDiskKey, ChildId)>) {
        if msgs.is_empty() {
            return (current, None);
        }
        let safe = self.superblock.safe_to_overwrite_in_place(current);
        if !safe {
            current = self.copy_node(&current);
            // safe = true;
        };
        let mut node = self.pool.acquire(&current);
        let old_current = current;
        let pivots = match &mut node.node_inner {
            NodeType::Leaf(leaf) => {
                msgs.into_iter().for_each(|(key, msg)| leaf.apply(key, msg));
                let mut pivots = None;
                if leaf.is_node_full() {
                    let right_sib_id = self.superblock.alloc();
                    let (right_sib, median) = leaf.split();
                    self.pool.put(right_sib_id, right_sib);
                    pivots = Some((median, right_sib_id));
                }
                assert!(!leaf.is_node_full());
                // pivots.reverse();
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
            NodeType::Internal(internal) => {
                let mut merging_possible = HashSet::new();
                let first_key = msgs.first_key_value().unwrap().0;
                let last_key = msgs.last_key_value().unwrap().0;
                let first_child = internal.find_child_with_key(first_key);
                let last_child = internal.find_child_with_key(last_key);
                if first_child == last_child
                    && self.pool.contains(&first_child)
                    && self.pool.get(&first_child).dirty()
                {
                    // internal.msg_buffer
                    //     .extract_if(|k, _v| internal.find_child_with_key(k) == last_child).for_each(
                    //     |(k, v)| {msgs.insert(k, v);}
                    //     );
                    // internal.msg_buffer.refresh_size();
                    msgs.keys().for_each(|k| {
                        internal.msg_buffer.remove(k);
                    });
                    let (child_id, new_pivots) = self.send_msgs_to_subtree(first_child, msgs);
                    if new_pivots.is_none() && self.merging_possible(&child_id) {
                        merging_possible.insert(child_id);
                    }
                    internal.update_pivots(first_child, child_id, new_pivots)
                } else {
                    internal.merge_buffers(msgs);
                    if internal.is_msg_buffer_full() {
                        let msgs_map = internal.prepare_msg_flush();
                        for (c, msgs) in msgs_map {
                            let (child_id, new_pivots) = self.send_msgs_to_subtree(c, msgs);
                            if new_pivots.is_none() && self.merging_possible(&child_id) {
                                merging_possible.insert(child_id);
                            }
                            internal.update_pivots(c, child_id, new_pivots)
                        }
                    }
                }

                // self.merge(merging_possible, internal);

                let mut pivots = None;
                if internal.is_pivots_full() {
                    let right_sib_id = self.superblock.alloc();
                    let (right_sib, median) = internal.split();
                    self.pool.put(right_sib_id, right_sib);
                    pivots = Some((median, right_sib_id));
                }
                // pivots.reverse();
                assert!(!internal.is_pivots_full());
                pivots
            }
            _ => unimplemented!(),
        };

        let res = if node.is_root() && !pivots.is_none() {
            // Do it in while loop
            let parent = Node::new_internel_root(current, pivots);
            let parent_id = self.superblock.alloc();
            node.unset_root();
            self.pool.put(parent_id, parent);
            (parent_id, None)
        } else {
            (current, pivots)
        };
        self.pool.release(old_current, node);
        res
    }

    pub fn merging_possible(&mut self, child_id: &ChildId) -> bool {
        let node = self.pool.get(child_id);
        node.merging_possible()
    }

    pub fn merge(&mut self, mut merging_possible: HashSet<ChildId>, node: &mut InternalNode) {
        let mut pre_child = node.rightmost_child;
        let mut deleted_keys = vec![];

        for (k, &c) in node.pivot_map.iter().rev() {
            if merging_possible.contains(&pre_child) && self.pool.get(&c).merging_possible() {
                let mut c = c;
                let safe = self.superblock.safe_to_overwrite_in_place(c);
                if !safe {
                    c = self.copy_node(&c);
                    // safe = true;
                };
                let pre_node = self.pool.acquire(&pre_child);
                let current = self.pool.get_mut(&c);
                current.merge(pre_node);
                deleted_keys.push((k.clone(), c));
                if current.merging_possible() {
                    // merging_possible.insert(c);
                }
                merging_possible.remove(&c);
                merging_possible.remove(&pre_child);
            }
            pre_child = c;
        }
        // for (k, c) in deleted_keys {
        //     let cursor = node.pivot_map.lower_bound(std::ops::Bound::Excluded(&k));
        //     let next_key = cursor.key().map(|k| k.clone());
        //     if let Some(next_key) = next_key {
        //         node.pivot_map.insert(next_key, c);
        //     } else {
        //         unreachable!();
        //     }
        //     node.pivot_map.remove(&k);
        // }
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        let key = OnDiskKey::new(key);
        let msg_data = MessageData::new(MessageType::Delete, vec![]);
        let mut buf = MsgBuffer::new();
        buf.insert(key, msg_data);
        // Merging
    }

    pub fn upsert(&mut self, key: Vec<u8>, val: Vec<u8>) {
        let key = OnDiskKey::new(key);
        let msg_data = MessageData::new(MessageType::Upsert, val);

        let mut buf = MsgBuffer::new();
        buf.insert(key, msg_data);

        // logging
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let key = OnDiskKey::new(key.to_vec());
        self.get_from_subtree(&key, self.root)
    }

    // Stupid borrow checker
    // fn get_from_subtree(
    //     &mut self,
    //     key: &OnDiskKey,
    //     page: ChildId,
    // ) -> Option<&[u8]> {
    //     let next_child;
    //     let node = self.pool.get(&page);
    //     let res = match &node.node_inner {
    //         NodeType::Leaf(leaf) => {
    //             leaf.get(key)
    //         }
    //         NodeType::Internal(internal) => {
    //             match internal.get(key) {
    //                 Ok(slice) => Some(slice),
    //                 Err(child_id) => {next_child = child_id; None}
    //             }
    //         }
    //         _ => {unimplemented!()}
    //     };
    //
    //     if let Some(slice) = res {
    //         return Some(slice)
    //     }
    //     self.get_from_subtree(key, next_child)
    // }

    fn get_from_subtree(&mut self, key: &OnDiskKey, page: ChildId) -> Option<Vec<u8>> {
        let node = self.pool.get(&page);
        match &node.node_inner {
            NodeType::Leaf(leaf) => leaf.get(key).map(|s| s.to_vec()),
            NodeType::Internal(internal) => match internal.get(key) {
                Ok(slice) => Some(slice.to_vec()),
                Err(child_id) => self.get_from_subtree(key, child_id),
            },
            _ => {
                unimplemented!()
            }
        }
    }

    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let cfg = CFG.get_or_init(|| crate::Args::default());
        let mut superblock = Superblock::new(&path);
        let mut pool = NodeCache::new(
            &superblock.storage_filename,
            true,
            cfg.buffer_size.try_into().unwrap(),
        );
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
        let cfg = CFG.get_or_init(|| crate::Args::default());
        if Superblock::exists(&path) {
            let superblock = Superblock::open(path);
            let mut pool = NodeCache::new(
                &superblock.storage_filename,
                false,
                cfg.buffer_size.try_into().unwrap(),
            );
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

    pub fn print_tree(&mut self) {
        let mut count = 0;
        let mut height = 0;
        let mut max_child_id = 0;
        let mut q = VecDeque::new();
        let mut new_queue = VecDeque::new();
        q.push_back(self.root);
        loop {
            while let Some(n) = q.pop_front() {
                count += 1;
                let node = self.pool.get(&n);
                let mut s = "".to_owned();

                match &node.node_inner {
                    NodeType::Internal(internel) => {
                        if node.is_root() {
                            s.push_str(&format!(
                                "root(pivot: {:?}); rightmost: {} ",
                                internel.pivot_map, internel.rightmost_child
                            ));
                        }
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
                        internel.pivot_map.iter().for_each(|(_, &c)| {
                            max_child_id = max_child_id.max(c);
                            new_queue.push_back(c)
                        });

                        max_child_id = max_child_id.max(internel.rightmost_child);
                        new_queue.push_back(internel.rightmost_child);
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
                info!("Node ID({}): {} | ", n, s);
            }
            height += 1;
            // info!();
            info!("=================================================================================================================================================");
            info!("count: {count}; height: {height}; max_child_id: {max_child_id}");
            if new_queue.is_empty() {
                break;
            } else {
                core::mem::swap(&mut q, &mut new_queue);
            }
        }
    }
}

// #[cfg(test)]

#[cfg(test)]
#[allow(dead_code)]
fn generate_test_file() {
    use std::collections::HashMap;
    use std::fs::File;
    // let file = File::create("test_map").unwrap();
    // serde_json::to_writer(file, &ref_map).unwrap();
    let file = File::create("test_map").unwrap();
    let test_cap = 480000;

    use rand::prelude::*;
    let mut rng = StdRng::seed_from_u64(69420);
    let mut ref_map = HashMap::with_capacity(test_cap);

    for _i in 0..test_cap {
        // let k = vec![rng.gen(), rng.gen(), rng.gen(), rng.gen()];
        // let v = vec![rng.gen(), rng.gen(), rng.gen(), rng.gen()];
        let k_val = rng.gen::<u64>();
        let v_val = rng.gen::<u64>();
        ref_map.insert(k_val, v_val);
    }
    let v: Vec<(u64, u64)> = ref_map.into_iter().collect();

    let mut file_for_cpp = File::create("test_inputs.txt").unwrap();
    use std::io::Write;
    v.iter()
        .for_each(|(k, _v)| writeln!(file_for_cpp, "Inserting {}", k).unwrap());
    serde_json::to_writer(file, &v).unwrap();
}

#[test]
fn test_both() {
    generate_test_file();
    // test_btree();
}
