use crate::types::{MessageData, MessageType, Serializable};
use core::mem::size_of;
use std::collections::{BTreeMap, HashMap};

use crate::error::Error;
use crate::page::{Page, PAGESIZE};
use crate::pager::PageId;
use crate::types::{OnDiskKey, OnDiskValue, PageOffset, SizedOnDisk};
use ser_derive::SizedOnDisk;
pub type ChildId = PageId;
pub type MsgBuffer = BTreeMap<OnDiskKey, MessageData>;
pub type PivotMap = BTreeMap<OnDiskKey, ChildId>;

const MAX_MSG_SIZE: PageOffset = PAGESIZE as PageOffset / 128;
const MAX_KEY_SIZE: PageOffset = PAGESIZE as PageOffset / 128;
const MAX_VAL_SIZE: PageOffset = PAGESIZE as PageOffset / 128;

#[derive(SizedOnDisk, Clone, Debug)]
pub struct LeafNode {
    map: BTreeMap<OnDiskKey, OnDiskValue>,
}

impl LeafNode {
    fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    fn get_meta_size(&self) -> PageOffset {
        true.size() + COM.size()
    }

    fn get_kv_avail(&self) -> PageOffset {
        self.get_kv_capacity() - self.map.size()
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

    pub fn split(&mut self) -> (Node, OnDiskKey) {
        let mut right_leaf = Self::new();
        while !right_leaf.is_node_full() && self.map.size() > self.get_kv_capacity() / 2 {
            if let Some((key, value)) = self.map.pop_last() {
                right_leaf.map.insert(key, value);
            } else {
                break;
            }
        }
        (
            Node {
                common_data: NodeCommon {
                    root: false,
                    dirty: true,
                },
                node_inner: NodeType::Leaf(right_leaf),
            },
            self.map.last_key_value().unwrap().0.clone(),
        )
        // (self.clone(), OnDiskKey::new(vec![]))
    }
}

impl Serializable for LeafNode {
    fn serialize(&self, destination: &mut [u8]) {
        let mut cursor = 0;
        serialize!(self.map, destination, cursor);
    }

    fn deserialize(src: &[u8]) -> Self {
        let mut cursor = 0;
        let map = deserialize!(BTreeMap<OnDiskKey, OnDiskValue>, src, cursor);
        Self { map }
    }
}

type PivotsLength = u16;

#[derive(SizedOnDisk, Clone, Debug)]
pub struct InternelNode {
    pub pivot_map: BTreeMap<OnDiskKey, ChildId>,
    rightmost_child: ChildId,
    pub msg_buffer: MsgBuffer,
    epsilon: f32,
}

impl InternelNode {
    pub fn get_msg_buffer_capacity(&self) -> PageOffset {
        (self.get_data_size() as f32 * self.epsilon) as PageOffset
    }

    pub fn merge_buffers(&mut self, mut msgs: MsgBuffer) {
        self.msg_buffer.append(&mut msgs);
    }

    fn get_msg_buffer_avail(&self) -> PageOffset {
        let cap = self.get_msg_buffer_capacity();
        let current_size = self.msg_buffer.size();
        if cap >= current_size {
            cap - current_size
        } else {
            0
        }
    }

    pub fn get_pivots_capacity(&self) -> PageOffset {
        self.get_data_size() - self.get_msg_buffer_capacity()
    }

    pub fn get_pivots_avail(&self) -> PageOffset {
        self.get_pivots_capacity() - self.pivot_map.size()
    }

    fn get_meta_size(&self) -> PageOffset {
        true.size() + self.epsilon.size()
    }

    fn get_data_size(&self) -> PageOffset {
        PAGESIZE as PageOffset - COM.size() - self.get_meta_size()
    }

    pub fn is_msg_buffer_full(&self, new_entry_size: PageOffset) -> bool {
        self.get_msg_buffer_avail() == 0 || self.get_msg_buffer_avail() < new_entry_size
    }

    pub fn is_pivots_full(&self) -> bool {
        self.get_pivots_avail() < MAX_KEY_SIZE + size_of::<ChildId>()
    }

    pub fn insert_msg(&mut self, key: OnDiskKey, msg: MessageData) {
        self.msg_buffer.insert(key, msg);
        // debug_assert!(self.msg_buffer.size() <= self.get_msg_buffer_capacity());
    }

    pub fn find_child_with_most_msgs(&self) -> ChildId {
        let mut record: HashMap<ChildId, PageOffset> = HashMap::new();
        self.msg_buffer.iter().for_each(|(k, v)| {
            let child_id = self.find_child_with_key(k);
            *record.entry(child_id).or_default() += k.size() + v.size();
        });
        let (child, size) = record.into_iter().max_by_key(|(_x, size)| *size).unwrap();
        child
    }

    fn find_child_with_key(&self, k: &OnDiskKey) -> ChildId {
        let c = self.pivot_map.lower_bound(std::ops::Bound::Included(&k));
        c.value().map(|&i| i).unwrap_or(self.rightmost_child)
    }

    pub fn collect_msg_to_child(&mut self, c: ChildId) -> MsgBuffer {
        let keys: Vec<_> = self
            .msg_buffer
            .iter()
            .filter(|(k, _v)| self.find_child_with_key(k) == c)
            .map(|(k, _v)| k.clone())
            .collect();
        let mut msgs = MsgBuffer::new();
        keys.into_iter().for_each(|k| {
            let v = self.msg_buffer.remove(&k).unwrap();
            msgs.insert(k, v);
        });
        msgs
    }

    pub fn new_internel_root(
        pivot_map: BTreeMap<OnDiskKey, ChildId>,
        rightmost_child: ChildId,
    ) -> Self {
        Self {
            pivot_map,
            rightmost_child,
            msg_buffer: BTreeMap::new(),
            epsilon: 0.5,
        }
    }

    pub fn update_pivots(
        &mut self,
        old_child: ChildId,
        child_id: ChildId,
        new_pivots: Vec<(OnDiskKey, ChildId)>,
    ) {
        if new_pivots.is_empty() {
            if self.rightmost_child == child_id {
                self.rightmost_child = child_id;
            } else {
                self.pivot_map
                    .iter_mut()
                    .find(|(k, v)| **v == old_child)
                    .map(|(_k, v)| *v = child_id);
            }
        } else {
            let (mut pivot_map, rightmost_child) = convert_pivot(child_id, new_pivots);
            let cursor = self.pivot_map.lower_bound(std::ops::Bound::Included(
                pivot_map.last_key_value().unwrap().0,
            ));
            if let Some(k) = cursor.key().map(|x| x.clone()) {
                self.pivot_map.insert(k, rightmost_child);
            } else {
                self.rightmost_child = rightmost_child;
            }
            self.pivot_map.append(&mut pivot_map);
        }
    }

    pub fn split(&mut self) -> (Node, OnDiskKey) {
        // match &mut self.node_inner {
        // }
        let len = self.pivot_map.len();
        debug_assert!(len >= 3);
        let median = len / 2;
        let key_next = self.pivot_map.keys().nth(median + 1).unwrap().clone();
        let msgs = self.msg_buffer.split_off(&key_next);
        let new_pivots = self.pivot_map.split_off(&key_next);
        let (median_key, rightmost_child) = self.pivot_map.pop_last().unwrap().clone();
        let original_rightmost = self.rightmost_child;
        self.rightmost_child = rightmost_child;
        (
            Node {
                common_data: COM,
                node_inner: NodeType::Internel(InternelNode::new(
                    self.epsilon,
                    new_pivots,
                    original_rightmost,
                    msgs,
                )),
            },
            median_key,
        )
        // (self.clone(), OnDiskKey::new(vec![]))
    }

    fn new(
        epsilon: f32,
        pivot_map: PivotMap,
        rightmost_child: ChildId,
        msg_buffer: MsgBuffer,
    ) -> Self {
        Self {
            epsilon,
            msg_buffer,
            pivot_map,
            rightmost_child,
        }
    }
}

impl Serializable for InternelNode {
    fn serialize(&self, destination: &mut [u8]) {
        let mut cursor = 0;
        serialize!(self.epsilon, destination, cursor);
        serialize!(self.pivot_map, destination, cursor);
        serialize!(self.rightmost_child, destination, cursor);
        serialize!(self.msg_buffer, destination, cursor);
    }

    fn deserialize(src: &[u8]) -> Self {
        let mut cursor = 0;
        let epsilon = deserialize!(f32, src, cursor);
        let pivot_map = deserialize!(PivotMap, src, cursor);
        let rightmost_child = deserialize!(ChildId, src, cursor);
        let msg_buffer = deserialize!(MsgBuffer, src, cursor);
        Self {
            epsilon,
            pivot_map,
            rightmost_child,
            msg_buffer,
        }
    }
}

#[derive(Clone, Debug)]
pub enum NodeType {
    Leaf(LeafNode),
    Internel(InternelNode),
    Test,
}

impl SizedOnDisk for NodeType {
    fn size(&self) -> PageOffset {
        true.size()
            + match self {
                Self::Leaf(leaf) => leaf.size(),
                Self::Internel(i) => i.size(),
                _ => unimplemented!(),
            }
    }
}

impl Serializable for NodeType {
    fn serialize(&self, destination: &mut [u8]) {
        let mut cursor = 0;
        let is_leaf = matches!(self, Self::Leaf(_));
        serialize!(is_leaf, destination, cursor);
        match self {
            Self::Leaf(leaf) => {
                serialize!(leaf, destination, cursor);
            }
            Self::Internel(i) => {
                serialize!(i, destination, cursor);
            }
            _ => unimplemented!(),
        }
    }

    fn deserialize(src: &[u8]) -> Self {
        let mut cursor = 0;
        let is_leaf = deserialize!(bool, src, cursor);
        if is_leaf {
            Self::Leaf(deserialize!(LeafNode, src, cursor))
        } else {
            Self::Internel(deserialize!(InternelNode, src, cursor))
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
        let mut cursor = 0;
        serialize!(self.root, destination, cursor);
    }

    fn deserialize(src: &[u8]) -> Self {
        let mut cursor = 0;
        let root = deserialize!(bool, src, cursor);
        Self { dirty: false, root }
    }
}

#[derive(Clone, SizedOnDisk, Debug)]
pub struct Node {
    pub node_inner: NodeType,
    common_data: NodeCommon,
}

impl Node {
    pub fn new_internel_root(child_id: ChildId, pivots: Vec<(OnDiskKey, ChildId)>) -> Self {
        let (pivot_map, rightmost_child) = convert_pivot(child_id, pivots);
        Self {
            common_data: NodeCommon {
                root: true,
                dirty: true,
            },
            node_inner: NodeType::Internel(InternelNode::new_internel_root(
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
            NodeType::Internel(internal) => {
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

    pub fn need_pre_split(&self, msg_size: PageOffset) -> bool {
        match &self.node_inner {
            NodeType::Leaf(_leaf) => false,
            NodeType::Internel(internel) => {
                if !internel.is_msg_buffer_full(msg_size) {
                    false
                } else {
                    if internel.is_pivots_full() {
                        true
                    } else {
                        false
                    }
                    // if pivot full, split pivot; else flush msg buffer
                }
            }
            _ => unimplemented!(),
        }
    }

    pub fn well_formed(&self) -> bool {
        match &self.node_inner {
            NodeType::Internel(internel) => {
                !internel.is_pivots_full() && !internel.is_msg_buffer_full(0)
            }
            NodeType::Leaf(leaf) => leaf.is_node_full(),
            _ => unimplemented!(),
        }
    }
    // Return right sibling and new parent
}

const NODE_META_OFFSET: usize = 0;

impl TryFrom<&Page> for Node {
    type Error = Error;
    fn try_from(value: &Page) -> Result<Self, Self::Error> {
        let mut cursor = NODE_META_OFFSET;
        let common_data = deserialize!(NodeCommon, value, cursor);
        let node_inner = deserialize!(NodeType, value, cursor);
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
        let mut cursor = NODE_META_OFFSET;
        serialize!(value.common_data, page, cursor);
        serialize!(value.node_inner, page, cursor);
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
