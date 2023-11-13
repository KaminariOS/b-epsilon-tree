use crate::pager::PageId;
use crate::types::{Serializable, SizedOnDisk};
use crate::{deserialize, deserialize_with_var, serialize};
use ser_derive::SizedOnDisk;
// enum PageType {
//     INVALID,
//     BRANCH,
//     SUPERBLOCK,
//     FILTER,
// }

pub trait PageAllocator {
    fn alloc(&mut self) -> PageId;
    fn dealloc(&mut self, _addr: PageId) {}
}

#[derive(Default, SizedOnDisk, Clone, Debug)]
pub struct SimpleAllocator {
    counter: PageId,
}

impl Serializable for SimpleAllocator {
    fn serialize(&self, destination: &mut [u8]) {
        serialize!(self.counter, destination);
    }

    fn deserialize(src: &[u8]) -> Self {
        deserialize_with_var!(counter, PageId, src);
        Self { counter }
    }
}

impl PageAllocator for SimpleAllocator {
    fn alloc(&mut self) -> PageId {
        self.counter += 1;
        self.counter
    }
}
