# B<sup>ε</sup> tree implementation in Rust

## Background

B<sup>ε</sup> tree is a write-optimized data structure described in [^fn].

## Design overview
From bottom up:

- Pager: a wrapper of a [std::fs::File](https://doc.rust-lang.org/std/fs/struct.File.html) object exposing methods from reading and writing data in PAGESIZE.
- Serialization layer: traits, macros, and implementations for on-disk size calculation and (de)serialization of objects.
- Node cache layer: a fixed-size buffer pool for `Node` objects based on LRU queue.
- Superblock: contains metadata of the tree
- B<sup>ε</sup> tree implemenation:
  - Every `page` is an on-disk representation of an in-memory `Node`.
  - Every `Node`/`Page` has a unique `PageId`
  - Variable-size keys and values(byte array)
  - Leaf node, pivots, and message buffers are all represented as [std::collections::BTreemap](https://doc.rust-lang.org/std/collections/struct.BTreeMap.html) in memory and SSTables on disk.
  - Well-formedness: a leaf node is well-formed if its on-disk size is not larger than PAGESIZE; an internal node is well-formed if both pivots and message buffer have an on-disk size not larger than their capacities(determined by PAGESIZE and ε)
  - Core method:
      ```
      fn send_msgs_to_subtree(
      &mut self,
      mut current: ChildId,
      msgs: MsgBuffer,
      ) -> (ChildId, Vec<(OnDiskKey, ChildId)>)
      ```


  This method accepts an ID of the root node of the subtree and a collection of messages. It returns a new ID of the root node(Copy-on-write) and a collection of new pivots from splitting.

  Pseudo code:
    1. if the current node is `InternalNode`,
       
      1. if all incoming messages go to the same child, sanitize current buffer and then flush directly with no merging; else: merge `msgs` with its own message buffer.
      2. While the current message buffer is not well-formed, flush all messages in buffer and merge the returned new pivots to the current pivot map.
      3. While the current pivots map is not well-formed, keep splitting.
      4. Return new pivots
    2. if the current node is `LeafNode`, apply all messages to the current tree. Keep splitting until the current node is well-formed. Return new pivots.

[^fn]: Bender, Michael A., Martín Farach-Colton, William Jannen, Rob Johnson, Bradley C. Kuszmaul, Donald E. Porter, Jun Yuan and Yang Zhan. “An Introduction to Bε-trees and Write-Optimization.” login Usenix Mag. 40 (2015): n. pag.
