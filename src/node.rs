use std::collections::BTreeMap;

use crate::error::Error;
use crate::page::Page;
use crate::types::{MessageType, OnDiskKey, OnDiskValue};

struct LeafNode {
    map: BTreeMap<OnDiskKey, OnDiskValue>,
}

struct InternelNode {
    pivots: Vec<OnDiskKey>,
    msg_buffer: BTreeMap<OnDiskKey, (OnDiskValue, MessageType)>,
}

enum NodeType {
    Leaf(LeafNode),
    Internel(InternelNode),
    Test,
}

pub struct Node {
    node_inner: NodeType,
    dirty: bool,
    root: bool,
    epsilon: f32,
}

impl Node {
    pub fn dirty(&self) -> bool {
        self.dirty
    }

    pub fn dirt(&mut self) {
        self.dirty = true;
    }

    pub fn clear(&mut self) {
        self.dirty = false;
    }

    pub fn to_page(&self) -> Result<Page, Error> {
        self.try_into()
    }
}

impl TryFrom<&Page> for Node {
    type Error = Error;
    fn try_from(value: &Page) -> Result<Self, Self::Error> {
        Ok(Node {
            dirty: false,
            root: true,
            node_inner: NodeType::Test,
            epsilon: 0.,
        })
    }
}

impl TryFrom<Page> for Node {
    type Error = Error;
    fn try_from(value: Page) -> Result<Self, Self::Error> {
        Ok(Node {
            dirty: false,
            root: true,
            node_inner: NodeType::Test,
            epsilon: 0.,
        })
    }
}

impl TryFrom<&Node> for Page {
    type Error = Error;
    fn try_from(value: &Node) -> Result<Self, Self::Error> {
        Ok(Page::default())
    }
}
