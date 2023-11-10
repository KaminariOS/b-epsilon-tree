use crate::pager::PageId;
use crate::types::{Serializable, SizedOnDisk};
use crate::{deserialize, serialize};
use ser_derive::SizedOnDisk;
enum PageType {
    INVALID,
    BRANCH,
    SUPERBLOCK,
    FILTER,
}

pub trait PageAllocator {
    fn alloc(&mut self) -> PageId;
    fn dealloc(&mut self, _addr: PageId) {}
}

#[derive(Default, SizedOnDisk, Clone)]
pub struct SimpleAllocator {
    counter: PageId,
}

impl Serializable for SimpleAllocator {
    fn serialize(&self, destination: &mut [u8]) {
        let mut cursor = 0;
        serialize!(self.counter, destination, cursor);
    }

    fn deserialize(src: &[u8]) -> Self {
        let mut cursor = 0;
        let counter = deserialize!(PageId, src, cursor);
        Self { counter }
    }
}

impl PageAllocator for SimpleAllocator {
    fn alloc(&mut self) -> PageId {
        self.counter += 1;
        self.counter
    }
}
