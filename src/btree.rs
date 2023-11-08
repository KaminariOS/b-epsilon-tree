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
