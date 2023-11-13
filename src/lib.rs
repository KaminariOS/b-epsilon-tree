#![feature(is_sorted)]
#![feature(btree_cursors)]

mod allocator;
mod mini_allocator;

mod betree;
pub use betree::*;
mod log;
mod memtable;
#[macro_use]
mod types;
mod data;
mod error;
mod node;
mod page;
mod pager;
mod pool;
mod superblock;
mod wal;

// use page::PAGESIZE;
// const MAX_INLINE_KEY_SIZE: u64 = PAGESIZE / (PAGESIZE / 8);
// const MAX_INLINE_MESSAGE_SIZE: u64 = 35 * PAGESIZE / 100;
