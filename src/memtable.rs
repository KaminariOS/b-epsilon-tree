use crate::types::{MessageData, OnDiskKey};
use std::collections::BTreeMap;
use crate::types::SizedOnDisk;
use derive_more::{Deref, DerefMut};

#[derive(Deref, DerefMut)]
pub struct Memtable {
    max_disk_size: usize,
    #[deref_mut]
    #[deref]
    inner: BTreeMap<OnDiskKey, MessageData>,
}

impl Memtable {
    fn is_full(&self) -> bool {
        self.inner.size() >= self.max_disk_size
    }

    fn flush(&mut self) {
    }
}

