use crate::types::{MessageData, Serializable, VectorOnDisk};
use std::collections::BTreeMap;

use crate::error::Error;
use crate::page::Page;
use crate::pager::PageId;
use crate::types::{OnDiskKey, OnDiskValue, PageOffset, SizedOnDisk};
pub type ChildId = PageId;

SizedOnDiskImplForComposite! {
struct LeafNode {
    map: BTreeMap<OnDiskKey, OnDiskValue>,
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

SizedOnDiskImplForComposite! {
struct InternelNode {
    pivots: VectorOnDisk<OnDiskKey, PivotsLength>,
    children: VectorOnDisk<ChildId, PivotsLength>,
    msg_buffer: BTreeMap<OnDiskKey, MessageData>,
    epsilon: f32,
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

SizedOnDiskImplForComposite! {
#[derive(Default)]
struct NodeCommon {
    root: bool,
    dirty: bool,
}
}

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
