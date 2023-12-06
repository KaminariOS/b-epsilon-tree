use crate::types::{MessageData, MessageType, Serializable};
use core::panic;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;

use crate::error::Error;
use crate::page::{Page, PAGESIZE};
use crate::pager::PageId;
use crate::types::{BTreeMapOnDisk, OnDiskKey, OnDiskValue, PageOffset, SizedOnDisk};
use ser_derive::SizedOnDisk;
pub type ChildId = PageId;
pub type MsgBuffer = BTreeMap<OnDiskKey, MessageData>;
pub type MsgBufferOnDisk = BTreeMapOnDisk<OnDiskKey, MessageData>;
pub type PivotMap = BTreeMap<OnDiskKey, ChildId>;
pub type PivotMapOnDisk = BTreeMapOnDisk<OnDiskKey, ChildId>;
pub type KVOnDisk = BTreeMapOnDisk<OnDiskKey, OnDiskValue>;

// const MAX_MSG_SIZE: PageOffset = PAGESIZE as PageOffset / 128;
const MAX_KEY_SIZE: PageOffset = PAGESIZE as PageOffset / 128;
// const MAX_VAL_SIZE: PageOffset = PAGESIZE as PageOffset / 128;

// type PivotsLength = u16;
const MAGIC: u64 = 0x18728742b91b43b;

#[derive(SizedOnDisk, Clone, Debug)]
pub struct LeafNode {
    map: KVOnDisk,
}

impl LeafNode {
    fn new() -> Self {
        Self {
            map: KVOnDisk::new(),
        }
    }

    pub fn get(&self, key: &OnDiskKey) -> Option<&[u8]> {
        self.map.get(key).map(|v| v.as_slice())
    }

    fn get_meta_size(&self) -> PageOffset {
        true.size() + COM.size() + MAGIC.size()
    }

    pub fn get_kv_capacity(&self) -> PageOffset {
        PAGESIZE as PageOffset - self.get_meta_size()
    }

    pub fn is_node_full(&self) -> bool {
        self.size() > self.get_kv_capacity()
    }

    pub fn apply(&mut self, key: OnDiskKey, msg: MessageData) {
        let MessageData { ty, val } = msg;
        match ty {
            MessageType::Insert => {
                self.map.insert(key, val);
            }
            MessageType::Delete => {
                debug_assert!(val.is_empty());
                self.map.remove(&key);
                // TODO: Need merging
            }
            MessageType::Upsert => {
                // TODO: Byte slice addition?
                unimplemented!()
            }
        }
    }

    fn well_formed(&self) -> bool {
        !self.is_node_full()
    }

    fn merge(&mut self, other: Self) {
        self.map.append(&mut other.map.to_inner());
    }

    pub fn split(&mut self) -> (Node, OnDiskKey) {
        let mut right_leaf = Self::new();
        while !right_leaf.is_node_full() && self.map.size() > self.get_kv_capacity() / 2 {
            if let Some((key, value)) = self.map.pop_last() {
                right_leaf.map.insert(key, value);
            } else {
                break;
            }
        }

        let new_node_first = right_leaf.map.first_key_value().unwrap().0.clone();
        let new_node = Node {
            common_data: NodeCommon {
                root: false,
                dirty: true,
            },
            node_inner: NodeType::Leaf(right_leaf),
        };
        debug_assert!(new_node.well_formed());
        debug_assert!(self.well_formed());
        (new_node, new_node_first)
        // (self.clone(), OnDiskKey::new(vec![]))
    }
}

impl Serializable for LeafNode {
    fn serialize(&self, destination: &mut [u8]) {
        debug_assert!(self.well_formed());
        serialize!(self.map, destination);
    }

    fn deserialize(src: &[u8]) -> Self {
        deserialize_with_var!(map, BTreeMap<OnDiskKey, OnDiskValue>, src);
        Self { map: map.into() }
    }
}

#[derive(SizedOnDisk, Clone, Debug)]
pub struct InternalNode {
    pub pivot_map: PivotMapOnDisk,
    pub rightmost_child: ChildId,
    pub msg_buffer: MsgBufferOnDisk,
    epsilon: f32,
}

impl InternalNode {
    pub fn get_msg_buffer_capacity(&self) -> PageOffset {
        (self.get_data_size() as f32 * self.epsilon) as PageOffset
    }

    pub fn merge_buffers(&mut self, mut msgs: MsgBuffer) {
        self.msg_buffer.append(&mut msgs);
    }

    pub fn well_formed(&self) -> bool {
        !self.is_pivots_full() && !self.is_msg_buffer_full()
    }

    pub fn get_pivots_capacity(&self) -> PageOffset {
        self.get_data_size() - self.get_msg_buffer_capacity()
    }

    pub fn get_pivots_avail(&self) -> PageOffset {
        self.get_pivots_capacity() - self.get_pivots_size()
    }

    pub fn get_pivots_size(&self) -> PageOffset {
        self.pivot_map.size() - self.rightmost_child.size()
    }

    fn get_meta_size(&self) -> PageOffset {
        true.size() + self.epsilon.size() + MAGIC.size()
    }

    fn get_data_size(&self) -> PageOffset {
        PAGESIZE as PageOffset - COM.size() - self.get_meta_size()
    }

    pub fn is_msg_buffer_full(&self) -> bool {
        self.get_msg_buffer_capacity() < self.msg_buffer.size()
    }

    pub fn is_pivots_full(&self) -> bool {
        self.get_pivots_capacity() < self.pivot_map.size() + self.rightmost_child.size()
        // self.get_pivots_avail() < MAX_KEY_SIZE + size_of::<ChildId>()
    }

    pub fn insert_msg(&mut self, key: OnDiskKey, msg: MessageData) {
        self.msg_buffer.insert(key, msg);
        // debug_assert!(self.msg_buffer.size() <= self.get_msg_buffer_capacity());
    }

    pub fn find_child_with_most_msgs(&self) -> (ChildId, Vec<OnDiskKey>) {
        debug_assert!(
            !self.msg_buffer.is_empty(),
            "Msg buffer size: {}",
            self.msg_buffer.size()
        );
        let mut record: _ = HashMap::<ChildId, PageOffset>::new();
        let mut record_keys = HashMap::<ChildId, Vec<&OnDiskKey>>::new();
        self.msg_buffer.iter().for_each(|(k, v)| {
            let child_id = self.find_child_with_key(k);
            *record.entry(child_id).or_default() += k.size() + v.size();
            record_keys.entry(child_id).or_default().push(k);
        });
        let (child, _size) = record.into_iter().max_by_key(|(_, size)| *size).unwrap();
        (
            child,
            record_keys
                .remove(&child)
                .unwrap()
                .into_iter()
                .map(|k| k.clone())
                .collect(),
        )
    }

    // Return msgs to flush, from right to left
    pub fn prepare_msg_flush(&mut self) -> Vec<(ChildId, MsgBuffer)> {
        let mut buffers = Vec::with_capacity(self.pivot_map.len());
        let mut pre_child = self.rightmost_child;
        for (key, child) in self.pivot_map.iter().rev() {
            let new_buffer = self.msg_buffer.split_off(key);
            if !new_buffer.is_empty() {
                // map.insert(pre_child, new_buffer);
                buffers.push((pre_child, new_buffer));
            }
            pre_child = *child;
        }
        if !self.msg_buffer.is_empty() {
            let mut leftmost_map = MsgBufferOnDisk::new();
            std::mem::swap(&mut leftmost_map, &mut self.msg_buffer);
            // map.insert(pre_child, leftmost_map.to_inner());
            buffers.push((pre_child, leftmost_map.to_inner()));
        }
        buffers
    }

    pub fn get(&self, key: &OnDiskKey) -> Result<&[u8], ChildId> {
        if let Some(slice) = self.msg_buffer.get(key).map(|m| m.val.as_slice()) {
            Ok(slice)
        } else {
            Err(self.find_child_with_key(key))
        }
    }

    pub fn find_child_with_key(&self, k: &OnDiskKey) -> ChildId {
        let c = self.pivot_map.lower_bound(std::ops::Bound::Excluded(&k));
        c.value().map(|&i| i).unwrap_or(self.rightmost_child)
    }

    pub fn collect_msgs(&mut self, keys: Vec<OnDiskKey>) -> MsgBuffer {
        // let keys: Vec<_> = self
        //     .msg_buffer
        //     .iter()
        //     .filter(|(k, _v)| self.find_child_with_key(k) == c)
        //     .map(|(k, _v)| k.clone())
        //     .collect();
        let mut msgs = MsgBuffer::new();
        keys.into_iter().for_each(|k| {
            let v = self.msg_buffer.remove(&k).unwrap();
            msgs.insert(k, v);
        });
        msgs
    }

    pub fn new_internel_root(pivot_map: PivotMap, rightmost_child: ChildId) -> Self {
        Self {
            pivot_map: pivot_map.into(),
            rightmost_child,
            msg_buffer: MsgBufferOnDisk::new(),
            epsilon: 0.5,
        }
    }

    pub fn update_pivots(
        &mut self,
        old_child: ChildId,
        child_id: ChildId,
        new_pivots: Option<(OnDiskKey, ChildId)>,
    ) {
        // assert!(new_pivots.len() <= 1);
        if new_pivots.is_none() {
            if old_child == child_id {
                return;
            }
            if self.rightmost_child == old_child {
                self.rightmost_child = child_id;
            } else {
                let k = self
                    .pivot_map
                    .iter()
                    .find(|(_, v)| **v == old_child)
                    .map(|(k, _)| k.clone())
                    .unwrap();
                self.pivot_map.insert(k, child_id);
            }
        } else if let Some((key, right)) = new_pivots {
            // let (mut pivot_map, rightmost_child) = convert_pivot(child_id, new_pivots);
            let cursor = self.pivot_map.lower_bound(std::ops::Bound::Excluded(&key));
            if let Some((k, v)) = cursor.key_value().map(|(k, v)| (k.clone(), *v)) {
                debug_assert_eq!(v, old_child);
                // debug_assert!(pivot_map.last_key_value().unwrap().0 <= &k);
                self.pivot_map.insert(k, right);
            } else {
                debug_assert_eq!(self.rightmost_child, old_child);
                self.rightmost_child = right;
            }
            self.pivot_map.insert(key, child_id);
            // self.pivot_map.append(&mut pivot_map);
        }
    }

    pub fn split(&mut self) -> (Node, OnDiskKey) {
        let len = self.pivot_map.len();
        debug_assert!(len >= 3);
        let median = len / 2;
        let key_next = self.pivot_map.keys().nth(median + 1).unwrap().clone();
        let new_pivots = self.pivot_map.split_off(&key_next);
        let (median_key, rightmost_child) = self.pivot_map.pop_last().unwrap();
        let mut msgs = self.msg_buffer.split_off(&median_key);
        let msg = msgs.remove_entry(&median_key);
        msg.map(|(k, m)| msgs.insert(k, m));
        let original_rightmost = self.rightmost_child;
        self.rightmost_child = rightmost_child;
        let new_node = Node {
            common_data: COM,
            node_inner: NodeType::Internal(InternalNode::new(
                self.epsilon,
                new_pivots.into(),
                original_rightmost,
                msgs.into(),
            )),
        };
        debug_assert!(self.well_formed());
        debug_assert!(new_node.well_formed());
        (new_node, median_key)
        // (self.clone(), OnDiskKey::new(vec![]))
    }

    fn merge(&mut self, other: Self) {
        let Self {
            pivot_map,
            rightmost_child,
            msg_buffer,
            ..
        } = other;
        self.msg_buffer.append(&mut msg_buffer.to_inner());
        // assert pivots non-overlapping
        self.pivot_map.append(&mut pivot_map.to_inner());
        self.rightmost_child = self.rightmost_child.max(rightmost_child);
    }

    fn new(
        epsilon: f32,
        pivot_map: PivotMap,
        rightmost_child: ChildId,
        msg_buffer: MsgBuffer,
    ) -> Self {
        Self {
            epsilon,
            msg_buffer: msg_buffer.into(),
            pivot_map: pivot_map.into(),
            rightmost_child,
        }
    }
}

impl Serializable for InternalNode {
    fn serialize(&self, destination: &mut [u8]) {
        debug_assert!(self.well_formed());
        let mut _cursor = 0;
        serialize!(self.epsilon, destination, _cursor);
        serialize!(self.pivot_map, destination, _cursor);
        serialize!(self.rightmost_child, destination, _cursor);
        serialize!(self.msg_buffer, destination, _cursor);
    }

    fn deserialize(src: &[u8]) -> Self {
        let mut _cursor = 0;
        let epsilon = deserialize!(f32, src, _cursor);
        let pivot_map = deserialize!(PivotMap, src, _cursor);
        let rightmost_child = deserialize!(ChildId, src, _cursor);
        let msg_buffer = deserialize!(MsgBuffer, src, _cursor);
        let new_node = Self {
            epsilon,
            pivot_map: pivot_map.into(),
            rightmost_child,
            msg_buffer: msg_buffer.into(),
        };
        debug_assert!(new_node.well_formed());
        new_node
    }
}

#[derive(Clone, Debug)]
pub enum NodeType {
    Leaf(LeafNode),
    Internal(InternalNode),
    Test,
}

impl SizedOnDisk for NodeType {
    fn size(&self) -> PageOffset {
        true.size()
            + match self {
                Self::Leaf(leaf) => leaf.size(),
                Self::Internal(i) => i.size(),
                _ => unimplemented!(),
            }
    }
}

impl Serializable for NodeType {
    fn serialize(&self, destination: &mut [u8]) {
        let mut _cursor = 0;
        let is_leaf = matches!(self, Self::Leaf(_));
        serialize!(is_leaf, destination, _cursor);
        match self {
            Self::Leaf(leaf) => {
                serialize!(leaf, destination, _cursor);
            }
            Self::Internal(i) => {
                serialize!(i, destination, _cursor);
            }
            _ => unimplemented!(),
        }
    }

    fn deserialize(src: &[u8]) -> Self {
        let mut _cursor = 0;
        let is_leaf = deserialize!(bool, src, _cursor);
        if is_leaf {
            Self::Leaf(deserialize!(LeafNode, src, _cursor))
        } else {
            Self::Internal(deserialize!(InternalNode, src, _cursor))
        }
    }
}

#[derive(Default, Copy, SizedOnDisk, Clone, Debug)]
struct NodeCommon {
    root: bool,
    dirty: bool,
}

const COM: NodeCommon = NodeCommon {
    root: false,
    dirty: true,
};

impl Serializable for NodeCommon {
    fn serialize(&self, destination: &mut [u8]) {
        serialize!(self.root, destination);
    }

    fn deserialize(src: &[u8]) -> Self {
        let root = deserialize!(bool, src);
        Self { dirty: false, root }
    }
}

#[derive(Clone, SizedOnDisk, Debug)]
pub struct Node {
    pub node_inner: NodeType,
    common_data: NodeCommon,
}

impl Node {
    pub fn new_internel_root(child_id: ChildId, pivots: Option<(OnDiskKey, ChildId)>) -> Self {
        let mut pivot_map = PivotMap::new();
        let rightmost_child = if let Some((key, right)) = pivots {
            pivot_map.insert(key, child_id);
            right
        } else {
            child_id
        };
        //  convert_pivot(child_id, pivots);
        Self {
            common_data: NodeCommon {
                root: true,
                dirty: true,
            },
            node_inner: NodeType::Internal(InternalNode::new_internel_root(
                pivot_map,
                rightmost_child,
            )),
        }
    }

    pub fn dirty(&self) -> bool {
        self.common_data.dirty
    }

    pub fn dirt(&mut self) {
        self.common_data.dirty = true;
    }

    pub fn clear(&mut self) {
        self.common_data.dirty = false;
    }

    pub fn to_page(&self) -> Result<Page, Error> {
        self.try_into()
    }

    pub fn new_empty_leaf(root: bool) -> Self {
        Self {
            common_data: NodeCommon { root, dirty: true },
            node_inner: NodeType::Leaf(LeafNode::new()),
        }
    }

    pub fn is_root(&self) -> bool {
        self.common_data.root
    }

    pub fn unset_root(&mut self) {
        self.common_data.root = false;
    }

    pub fn get_key(&self, key: &OnDiskKey) -> Option<&OnDiskValue> {
        match &self.node_inner {
            NodeType::Internal(internal) => {
                internal
                    .msg_buffer
                    .get(key)
                    .and_then(|MessageData { ty, val }| match ty {
                        MessageType::Insert => Some(val),
                        MessageType::Delete => None,
                        MessageType::Upsert => {
                            // Need to flush this msg
                            unimplemented!()
                        }
                    })
            }
            NodeType::Leaf(leaf) => leaf.map.get(key),
            _ => {
                unimplemented!()
            }
        }
    }

    pub fn merging_possible(&self) -> bool {
        const SPLIT_THRESHOLD: usize = 4;
        match &self.node_inner {
            NodeType::Internal(internal) => {
                internal.get_pivots_size() <= internal.get_pivots_capacity() / SPLIT_THRESHOLD
                    && internal.msg_buffer.size()
                        <= internal.get_msg_buffer_capacity() / SPLIT_THRESHOLD
            }
            NodeType::Leaf(leaf) => leaf.size() <= leaf.get_kv_capacity() / SPLIT_THRESHOLD,
            _ => {
                unimplemented!()
            }
        }
    }

    pub fn merge(&mut self, other: Self) {
        debug_assert!(self.dirty());
        let Self { node_inner, .. } = other;
        match (&mut self.node_inner, node_inner) {
            (NodeType::Leaf(left), NodeType::Leaf(right)) => {
                left.merge(right);
            }
            (NodeType::Internal(left), NodeType::Internal(right)) => {
                left.merge(right);
            }
            _ => panic!("Merging different types of nodes"),
        }
    }

    // pub fn need_pre_split(&self, msg_size: PageOffset) -> bool {
    //     match &self.node_inner {
    //         NodeType::Leaf(_leaf) => false,
    //         NodeType::Internal(internel) => {
    //             if !internel.is_msg_buffer_full(msg_size) {
    //                 false
    //             } else {
    //                 if internel.is_pivots_full() {
    //                     true
    //                 } else {
    //                     false
    //                 }
    //                 // if pivot full, split pivot; else flush msg buffer
    //             }
    //         }
    //         _ => unimplemented!(),
    //     }
    // }

    pub fn well_formed(&self) -> bool {
        match &self.node_inner {
            NodeType::Internal(internel) => internel.well_formed(),
            NodeType::Leaf(leaf) => leaf.well_formed(),
            _ => unimplemented!(),
        }
    }
    // Return right sibling and new parent
}

const NODE_META_OFFSET: usize = 0;

impl TryFrom<&Page> for Node {
    type Error = Error;
    fn try_from(value: &Page) -> Result<Self, Self::Error> {
        let mut _cursor = NODE_META_OFFSET;
        deserialize_with_var!(magic, u64, value, _cursor);
        assert_eq!(magic, MAGIC);
        let common_data = deserialize!(NodeCommon, value, _cursor);
        let node_inner = deserialize!(NodeType, value, _cursor);
        Ok(Node {
            common_data,
            node_inner,
        })
    }
}

impl TryFrom<Page> for Node {
    type Error = Error;
    fn try_from(value: Page) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<&Node> for Page {
    type Error = Error;
    fn try_from(value: &Node) -> Result<Self, Self::Error> {
        let mut page = Page::default();
        let mut _cursor = NODE_META_OFFSET;
        debug_assert!(value.well_formed());
        // assert!(value.size() <= PAGESIZE as usize, "{:?}", value);
        serialize!(MAGIC, page, _cursor);
        serialize!(value.common_data, page, _cursor);
        serialize!(value.node_inner, page, _cursor);
        Ok(page)
    }
}

pub fn convert_pivot(child_id: ChildId, pivots: Vec<(OnDiskKey, ChildId)>) -> (PivotMap, ChildId) {
    let mut map = PivotMap::new();
    for i in 0..pivots.len() {
        map.insert(
            pivots[i].0.clone(),
            if i == 0 { child_id } else { pivots[i - 1].1 },
        );
    }
    let right = if pivots.is_empty() {
        child_id
    } else {
        pivots.last().unwrap().1
    };
    (map, right)
}
