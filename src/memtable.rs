use crate::types::{MessageData, OnDiskKey};
use std::collections::BTreeMap;
struct Memtable {
    inner: BTreeMap<OnDiskKey, MessageData>,
}
