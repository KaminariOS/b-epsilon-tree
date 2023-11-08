mod  allocator;
mod mini_allocator;

mod btree;
mod memtable;
mod log;
#[macro_use]
mod types;
mod data;
mod pager;
mod error;
mod page;
mod pool;
mod node;

use page::PAGESIZE;

const MAX_INLINE_KEY_SIZE: u64 = PAGESIZE / (PAGESIZE / 8);
const MAX_INLINE_MESSAGE_SIZE: u64 = 35 * PAGESIZE / 100;

