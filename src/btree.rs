use crate::{types::*, PAGESIZE, SizedOnDiskImplForComposite};

type table_index = u16;
type node_offset = u16;
type table_entry = node_offset;

const BTREE_SPLIT_THRESHOLD: u64 = PAGESIZE / 2;


/**
 * After a split, the free space in the left node may be fragmented.
 * If there's less than this much contiguous free space, then we also
 * defrag the left node.
 */
const BTREE_DEFRAGMENT_THRESHOLD: u64 = PAGESIZE / 4;

struct btree_hdr {
   prev_addr: u64,
   next_addr: u64,
   next_extent_addr: u64,
   generation: u64,
   height: u8,
   next_entry: node_offset,
   offsets: BytesOnDisk<table_entry, table_index>,
}

impl btree_hdr {
    fn get_table_entry(&self, i: usize) -> table_entry {
        self.offsets[i]
    }

    fn num_entries(&self) -> usize {
        self.offsets.len()
    }

    fn increment_height(&mut self) {
        self.height += 1;
    }
}


SizedOnDiskImplForComposite!{
    /**
      *************************************************************************
      BTree pivot data: Disk-resident structure
     
      Metadata for a pivot of an internal BTree node. Returned from an iterator
      of height > 0 in order to track amount of data stored in sub-trees, given
      by stuff like # of key/value pairs, # of bytes stored in the tree.
     
      Iterators at (height > 0) return this struct as a value for each pivot.
      *************************************************************************
     */
    struct btree_pivot_stats {
        // num_kvs: u32,
        key_bytes: u32,
        // message_bytes: u32
    }
}

SizedOnDiskImplForComposite!{
    struct btree_pivot_data {
        child_addr: u64,
        stats: btree_pivot_stats

    }
}


pub struct index_entry {
    pivot_data: btree_pivot_data,
    pivot: OnDiskKey,
}

impl SizedOnDisk for index_entry {
    fn size(&self) -> PageOffset {
        self.pivot_data.size() + self.pivot.len() 
    }
}



