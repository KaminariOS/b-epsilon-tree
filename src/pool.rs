use lru::LruCache;

use crate::{node::Node, pager::{PageId, Pager, SimplePager}, page::Page};


struct NodeCache {
    cache: LruCache<PageId, Node>,   
    pager: SimplePager,
}



impl NodeCache {
    fn get<'a>(&'a mut self, page_id: PageId) -> &'a Node {
        if self.cache.contains(&page_id) {
            return self.cache.get(&page_id).unwrap()
        } 
        else {
            let mut page = Page::default();
            self.pager.read(page_id, &mut page).unwrap();
            self.evict_one_if_full();
            let node: Node = page.try_into().unwrap();
            self.cache.put(page_id, node);
            self.cache.get(&page_id).unwrap()
        }
    }

    fn get_mut<'a>(&'a mut self, page_id: PageId) -> &'a mut Node {
        if self.cache.contains(&page_id) {
            return self.cache.get_mut(&page_id).unwrap()
        } 
        else {
            let mut page = Page::default();
            self.pager.read(page_id, &mut page).unwrap();
            self.evict_one_if_full();
            let node: Node = page.try_into().unwrap();
            self.cache.put(page_id, node);
            self.cache.get_mut(&page_id).unwrap()
        }
    }

    fn evict_one_if_full(&mut self) {
        if self.cache.len() == self.cache.cap().into() {
            let (page_id, node) = self.cache.pop_lru().unwrap();
            if node.dirty() {
                let page: Page = (&node).try_into().unwrap();
                // TODO flush all dirty children
                self.pager.write(page_id, &page);
            }
        }
    }

    fn flush(&mut self) {
            self.cache.iter_mut().filter(|(_, node)| node.dirty())

            .for_each(|(&p, n)| 
                      {
                          let data = (&*n).try_into().unwrap();
                            // flush from large page id to small id 
                         self.pager.write(p, &data).unwrap();
                         n.clear();
                      }
                      );
            self.pager.flush().unwrap();
    }
}

