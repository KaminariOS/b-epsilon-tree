use std::mem::size_of;

use crate::types::{OnDiskKey, BytesOnDisk, SizedOnDisk, PageOffset};

/**
  mini_meta_hdr -- Disk-resident structure
       The header of a meta_page in a mini_allocator. Keyed mini_allocators
       use entry_buffer and unkeyed ones use entry.
 */
struct mini_meta_hdr {
   next_meta_addr: u64 ,
   pos: u64,
   /// len is stored on disk as u32
   entry_buffer: BytesOnDisk<u8, u32>,
}

impl SizedOnDisk for mini_meta_hdr {
    fn size(&self) -> PageOffset {
        (
        size_of::<u64>() + 
        size_of::<u64>() 
        ) as PageOffset
            + self.entry_buffer.size()
    }
}


/**
  keyed_meta_entry -- Disk-resident structure
       Metadata for each extent stored in the extent list for a keyed
       mini_allocator. The key range for each extent goes from start_key to
       the start_key of its successor (the next keyed_meta_entry from the same
       batch).
 */
struct keyed_meta_entry {
    extent_addr: u64,
    batch: u8,
    start_key: OnDiskKey,
} 

impl SizedOnDisk for keyed_meta_entry {
    fn size(&self) -> PageOffset {
        (size_of::<u64>() + size_of::<u8>()) as PageOffset + self.start_key.size()
    }
}

#[repr(transparent)]
struct unkeyed_meta_entry {
   extent_addr: u64,
} 



