use ser_derive::SizedOnDisk;
use std::fs::File;
// use std::io::Write;

use crate::{types::Serializable, types::SizedOnDisk};

// use crate::page::PAGESIZE;
use crate::types::Message;

// const LOG_START: u64 = (SB_PAGE_ID + 1) * PAGESIZE;
// pub type LsnType = u64;

/// The distance between superblock page and the wal next_log_offset should be kept short by
/// periodic checkpointing and resetting next_log_offset
#[derive(Default, SizedOnDisk, Clone)]
#[allow(dead_code)]
pub struct Wal {
    #[dignore]
    unflushed_logs: Vec<Message>,
    next_log_offset: u64,
}

impl Serializable for Wal {
    fn serialize(&self, destination: &mut [u8]) {
        serialize!(self.next_log_offset, destination);
    }

    fn deserialize(src: &[u8]) -> Self {
        deserialize_with_var!(next_log_offset, u64, src);
        Self {
            next_log_offset,
            unflushed_logs: vec![],
        }
    }
}

#[allow(dead_code)]
impl Wal {
    pub fn new(_next_log_offset: u64) -> Self {
        Self {
            next_log_offset: 0,
            unflushed_logs: vec![],
        }
    }

    // If it is impossible to flush part of a file1
    pub fn flush(&mut self, fd: &mut File) {
        fd.sync_all().unwrap();
        // fd.seek(SeekFrom::Start(LOG_START + self.next_log_offset))?;
        self.unflushed_logs.clear();
    }
}
