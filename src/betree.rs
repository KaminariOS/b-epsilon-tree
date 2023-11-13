use crate::node::{MsgBuffer, Node, NodeType};
use crate::pool::NodeCache;
use crate::superblock;
use crate::types::MessageData;
use crate::types::{MessageType, OnDiskKey, SizedOnDisk};
use crate::{allocator::PageAllocator, node::ChildId};
use std::collections::{VecDeque, BTreeMap};
use std::path::Path;
use std::time::Instant;
use superblock::Superblock;

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
        let msg_data = MessageData::new(MessageType::Insert, val);

        let mut buf = MsgBuffer::new();
        buf.insert(key, msg_data);
        let (child_id, p) = self.send_msgs_to_subtree(self.root, buf);
        debug_assert!(p.is_empty());
        // while !res.1.is_empty() {
        // let node = self.pool.get_mut(self.root);
        // node.
        // }
        self.root = child_id;
        self.superblock.root = child_id;
    }

    /// Return new pivot, left, right child id if split
    /// Parent must not be full
    /// How to handle tons of msg? May need more than one split
    fn send_msgs_to_subtree(
        &mut self,
        mut current: ChildId,
        msgs: MsgBuffer,
    ) -> (ChildId, Vec<(OnDiskKey, ChildId)>) {
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
            NodeType::Internal(internal) => {
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
                while internal.is_msg_buffer_full() {
                    let (c, keys) = internal.find_child_with_most_msgs();
                    let (child_id, new_pivots) =
                        self.send_msgs_to_subtree(c, internal.collect_msgs(keys));
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

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let key = OnDiskKey::new(key.to_vec());
        self.get_from_subtree(&key, self.root)
    }

    // Fuck borrow checker
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
        let mut superblock = Superblock::new(&path);
        let mut pool = NodeCache::new(&superblock.storage_filename, true, 1000.try_into().unwrap());
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
            let mut pool = NodeCache::new(
                &superblock.storage_filename,
                false,
                10000.try_into().unwrap(),
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

    fn print_tree(&mut self) {
        let mut q = VecDeque::new();
        let mut new_queue = VecDeque::new();
        q.push_back(self.root);
        loop {
            while let Some(n) = q.pop_front() {
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
                        internel
                            .pivot_map
                            .iter()
                            .for_each(|(_, c)| new_queue.push_back(*c));
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
                print!("Node ID({}): {} | ", n, s);
            }
            println!();
            println!("=================================================================================================================================================");
            if new_queue.is_empty() {
                break;
            } else {
                core::mem::swap(&mut q, &mut new_queue);
            }
        }
    }
}

// #[cfg(test)]
pub fn test_btree() {
    // use rand::prelude::*;
    // use rand_chacha::ChaCha8Rng;
    // let mut rng = StdRng::seed_from_u64(69420);
    let mut betree = Betree::open("/tmp/test_betree");
    betree.print_tree();
    // println!("Superblock root: {}", betree.superblock.last_flushed_root);
    // let test_cap = 18010;
    // let mut ref_map = BTreeMap::new();

    use std::fs::File;
    // let file = File::create("test_map").unwrap();
    // serde_json::to_writer(file, &ref_map).unwrap();
    let file = File::open("test_map").unwrap();
    let v: Vec<(u64, u64)> = serde_json::from_reader(file).unwrap();
    // println!("first ten: {:?}", &v[..10]);
    let len = v.len();
    println!("Total Keys: {}", len);
    let time = Instant::now();
    for &(k_val, v_val) in &v {
        // let k = vec![rng.gen(), rng.gen(), rng.gen(), rng.gen()];
        // let v = vec![rng.gen(), rng.gen(), rng.gen(), rng.gen()];
        let k = k_val.to_be_bytes().to_vec();
        let v = v_val.to_be_bytes().to_vec();

        // ref_map.insert(k, v);
        // ref_map.insert(k_val, v_val);
        betree.insert(k, v);
    }
    let elapsed = time.elapsed();
    println!("Total time: {}s; OPS: {}", elapsed.as_secs(), len as u128 / elapsed.as_millis());
    // betree.print_tree();
    // v.iter().enumerate().for_each(|(i, &(k, v))| {
    //     let res = betree
    //         .get(&k.to_be_bytes().to_vec())
    //         .expect(&format!("Couldn't get betree for {}th: {}", i, k));
    //     assert_eq!(&res, &v.to_be_bytes().to_vec());
    // });

    // betree.flush();
    // core::mem::drop(betree);
    // let mut betree = Betree::open("/tmp/test_betree");
    //
    // ref_map.into_iter().for_each(
    //     |(k, v)|
    //     {
    //         let res = betree.get(&k).unwrap();
    //         assert_eq!(res, v);
    //     }
    //     );
    // assert!(false);
}

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
    serde_json::to_writer(file, &v).unwrap();
}

#[test]
fn test_both() {
    // generate_test_file();
    test_btree();
}
