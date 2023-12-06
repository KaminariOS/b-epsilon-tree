#![feature(is_sorted)]
#![feature(btree_cursors)]

#[macro_use] extern crate log;
mod allocator;
mod mini_allocator;

mod betree;
use std::sync::OnceLock;

pub use betree::*;
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

mod args;
pub use args::Args;

pub(crate) static CFG: OnceLock<Args> = OnceLock::new();

pub fn init_cfg(cfg: Option<Args>) {
    CFG.set(cfg.unwrap_or_default()).unwrap();
}

// use page::PAGESIZE;
// const MAX_INLINE_KEY_SIZE: u64 = PAGESIZE / (PAGESIZE / 8);
// const MAX_INLINE_MESSAGE_SIZE: u64 = 35 * PAGESIZE / 100;
