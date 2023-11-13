// use crate::types::SizedOnDisk;
// use crate::types::{MessageData, OnDiskKey};
// use derive_more::{Deref, DerefMut};
// use std::collections::BTreeMap;
//
// #[derive(Deref, DerefMut)]
// pub struct Memtable {
//     max_disk_size: usize,
//     #[deref_mut]
//     #[deref]
//     inner: BTreeMap<OnDiskKey, MessageData>,
// }
//
// impl Memtable {
//     fn is_full(&self) -> bool {
//         self.inner.size() >= self.max_disk_size
//     }
//
//     fn flush(&mut self) {}
// }
