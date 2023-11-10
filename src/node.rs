use crate::types::{MessageData, Serializable, VectorOnDisk};
use core::mem::size_of;
use std::collections::BTreeMap;

use crate::error::Error;
use crate::page::{Page, PAGESIZE};
use crate::pager::PageId;
use crate::types::{OnDiskKey, OnDiskValue, PageOffset, SizedOnDisk};
use ser_derive::SizedOnDisk;
pub type ChildId = PageId;

const MAX_MSG_SIZE: PageOffset = PAGESIZE as PageOffset / 128;
const MAX_KEY_SIZE: PageOffset = PAGESIZE as PageOffset / 128;
const MAX_VAL_SIZE: PageOffset = PAGESIZE as PageOffset / 128;

#[derive(SizedOnDisk, Clone)]
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
        true.size()
    }

    fn get_kv_avail(&self) -> PageOffset {
        self.get_kv_capacity() - self.map.size()
    }

    fn get_kv_capacity(&self) -> PageOffset {
        PAGESIZE as PageOffset - self.get_meta_size()
    }

    fn is_node_full(&self, new_entry_size: PageOffset) -> bool {
        self.get_kv_avail() < new_entry_size
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

#[derive(SizedOnDisk, Clone)]
struct InternelNode {
    pivots: VectorOnDisk<OnDiskKey, PivotsLength>,
    children: VectorOnDisk<ChildId, PivotsLength>,
    msg_buffer: BTreeMap<OnDiskKey, MessageData>,
    epsilon: f32,
}

impl InternelNode {
    fn get_msg_buffer_capacity(&self) -> PageOffset {
        (self.get_data_size() as f32 * self.epsilon) as PageOffset
    }

    fn get_msg_buffer_avail(&self) -> PageOffset {
        self.get_msg_buffer_capacity() - self.msg_buffer.size()
    }

    fn get_pivots_capacity(&self) -> PageOffset {
        self.get_data_size() - self.get_msg_buffer_capacity()
    }

    fn get_pivots_avail(&self) -> PageOffset {
        self.get_pivots_capacity() - self.pivots.size()
    }

    fn get_meta_size(&self) -> PageOffset {
        true.size() + self.epsilon.size()
    }

    fn get_data_size(&self) -> PageOffset {
        PAGESIZE as PageOffset - COM.size() - self.get_meta_size()
    }
}

impl InternelNode {
    pub fn is_msg_buffer_full(&self, new_entry_size: PageOffset) -> bool {
        self.get_msg_buffer_avail() < new_entry_size
    }

    pub fn is_pivots_full(&self) -> bool {
        self.get_pivots_avail() < MAX_KEY_SIZE + size_of::<ChildId>()
    }
}

impl Serializable for InternelNode {
    fn serialize(&self, destination: &mut [u8]) {
        let mut cursor = 0;
        serialize!(self.epsilon, destination, cursor);
        serialize!(self.pivots, destination, cursor);
        serialize!(self.children, destination, cursor);
        serialize!(self.msg_buffer, destination, cursor);
    }

    fn deserialize(src: &[u8]) -> Self {
        let mut cursor = 0;
        let epsilon = deserialize!(f32, src, cursor);
        let pivots = deserialize!(VectorOnDisk<OnDiskKey, PivotsLength>, src, cursor);
        let children = deserialize!(VectorOnDisk<ChildId, PivotsLength>, src, cursor);
        let msg_buffer = deserialize!(BTreeMap<OnDiskKey, MessageData>, src, cursor);
        Self {
            epsilon,
            pivots,
            children,
            msg_buffer,
        }
    }
}

#[derive(Clone)]
enum NodeType {
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

#[derive(Default, Copy, SizedOnDisk, Clone)]
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

pub struct Node {
    node_inner: NodeType,
    common_data: NodeCommon,
}

impl Node {
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
