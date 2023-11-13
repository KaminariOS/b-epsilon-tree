use std::collections::HashSet;
use std::{num::NonZeroUsize, path::Path};

use lru::LruCache;

use crate::{
    node::Node,
    page::Page,
    pager::{PageId, Pager, SimplePager},
};

pub struct NodeCache {
    cache: LruCache<PageId, Node>,
    pager: SimplePager,
    taken: HashSet<PageId>,
}

impl NodeCache {
    pub fn acquire(&mut self, page_id: &PageId) -> Node {
        self.get(page_id);
        let node = self.cache.pop(page_id).unwrap();
        self.taken.insert(*page_id);
        node
    }

    pub fn release(&mut self, page_id: PageId, mut node: Node) {
        node.dirt();
        self.taken.remove(&page_id);
        self.put(page_id, node);
    }

    pub fn new<P: AsRef<Path>>(path: P, create: bool, cap: NonZeroUsize) -> Self {
        let cache = LruCache::new(cap);
        let pager = if create {
            SimplePager::new(path)
        } else {
            SimplePager::open(path)
        }
        .unwrap();
        Self {
            cache,
            pager,
            taken: HashSet::new(),
        }
    }

    pub fn get<'a>(&'a mut self, page_id: &PageId) -> &'a Node {
        debug_assert!(!self.taken.contains(page_id));
        if self.cache.contains(&page_id) {
            return self.cache.get(&page_id).unwrap();
        } else {
            let mut page = Page::default();
            self.pager
                .read(page_id, &mut page)
                .expect(&format!("Failed to page: {}", page_id));
            self.evict_one_if_full();
            let node: Node = page.try_into().unwrap();
            self.cache.put(*page_id, node);
            self.cache.get(page_id).unwrap()
        }
    }

    #[allow(dead_code)]
    pub fn get_mut<'a>(&'a mut self, page_id: &PageId) -> &'a mut Node {
        debug_assert!(!self.taken.contains(page_id));
        let r = if self.cache.contains(&page_id) {
            return self.cache.get_mut(&page_id).unwrap();
        } else {
            let mut page = Page::default();
            self.pager.read(page_id, &mut page).unwrap();
            self.evict_one_if_full();
            let node: Node = page.try_into().unwrap();
            self.cache.put(*page_id, node);
            self.cache.get_mut(&page_id).unwrap()
        };
        r.dirt();
        r
    }

    fn evict_one_if_full(&mut self) {
        let len = self.cache.len();
        let cap = <NonZeroUsize as Into<usize>>::into(self.cache.cap());
        debug_assert!(len <= cap);
        if len == cap {
            let (page_id, node) = self.cache.pop_lru().unwrap();
            if node.dirty() {
                let page: Page = (&node).try_into().unwrap();
                // TODO flush all dirty children
                self.pager.write(&page_id, &page).unwrap();
            }
        }
    }

    /// Call put before write through
    pub fn write_through(&mut self, page_id: &PageId) {
        debug_assert!(!self.taken.contains(page_id));
        let page = if let Some(node) = self.cache.get_mut(page_id).filter(|n| n.dirty()) {
            let page: Page = (&*node).try_into().unwrap();
            node.clear();
            Some(page)
        } else {
            None
        };
        if let Some(page) = page {
            self.pager.write(page_id, &page).unwrap();
            self.flush();
        }
    }

    pub fn put(&mut self, page_id: PageId, mut node: Node) {
        debug_assert!(!self.taken.contains(&page_id));
        assert!(!self.cache.contains(&page_id));
        node.dirt();
        self.evict_one_if_full();
        self.cache.put(page_id, node);
    }

    pub fn flush(&mut self) {
        self.cache
            .iter_mut()
            .filter(|(_, node)| node.dirty())
            .for_each(|(p, n)| {
                let data = (&*n).try_into().unwrap();
                // flush from large page id to small id
                self.pager.write(p, &data).unwrap();
                n.clear();
            });
        self.pager.flush().unwrap();
    }
}
