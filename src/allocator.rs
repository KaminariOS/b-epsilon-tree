enum PageType {
    INVALID,
    BRANCH,
    SUPERBLOCK,
    FILTER,

}

trait Allocator {
    fn alloc(&mut self, addr: u64, page_type: PageType);
}
