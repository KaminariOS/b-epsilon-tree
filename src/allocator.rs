use crate::pager::PageId;
enum PageType {
    INVALID,
    BRANCH,
    SUPERBLOCK,
    FILTER,
}

trait PageAllocator {
    fn alloc(&mut self) -> PageId;
    fn dealloc(&mut self, _addr: PageId) {
    }
}

struct SimpleAllocator {
    counter: PageId
}

impl PageAllocator for SimpleAllocator {
    fn alloc(&mut self) -> PageId {
        self.counter += 1;
        self.counter
    }
}
