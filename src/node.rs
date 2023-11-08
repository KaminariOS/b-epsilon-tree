use std::collections::BTreeMap;

use crate::page::Page;
use crate::error::Error;
use crate::types::{OnDiskKey, OnDiskValue};

struct LeafNode {

}


struct InternelNode {
    map: BTreeMap<OnDiskKey, OnDiskValue>
}

enum NodeType {
    Leaf(LeafNode),
    Internel(InternelNode)
}

pub struct Node {
    dirty: bool,
    root: bool,
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
        Ok(Node { dirty: false, root: true })         
    }    
}

impl TryFrom<Page> for Node {
    type Error = Error;
    fn try_from(value: Page) -> Result<Self, Self::Error> {
        Ok(Node { dirty: false, root: true })         
    }    
}

impl TryFrom<&Node> for Page {
    type Error = Error;
    fn try_from(value: &Node) -> Result<Self, Self::Error> {
        Ok(Page::default())         
    }    
}
