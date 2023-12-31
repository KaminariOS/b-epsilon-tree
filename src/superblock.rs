use crate::{
    allocator::{PageAllocator, SimpleAllocator},
    error::Error,
    node::ChildId,
    page::Page,
    page::PAGESIZE,
    pager::PageId,
    types::{Serializable, SizedOnDisk},
    wal::Wal,
};
use std::io::{Read, Seek, SeekFrom};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
};

const MAGIC: u64 = 0x12f81ac;
/// On disk represenation(little endian):
/// MAGIC: 8 bytes
/// root: 8 bytes
/// last_checkpoint: 8 bytes
/// storage_filename
/// allocator
/// Wal
#[allow(dead_code)]
pub struct Superblock {
    pub root: PageId,
    pub last_flushed_root: PageId,
    last_checkpoint: u64,
    wal: Wal,
    pub storage_filename: String,
    fd: File,
    page: Page,
    pub allocator: SimpleAllocator,
}

const META_EXT: &str = ".storage";

pub const SB_PAGE_ID: u64 = 0;

impl Superblock {
    pub fn alloc(&mut self) -> PageId {
        self.allocator.alloc()
    }

    fn serialize(&mut self) {
        let mut _cursor = 0;
        let destination: &mut [u8] = (&mut self.page).into();
        serialize!(MAGIC, destination, _cursor);
        serialize!(self.root, destination, _cursor);
        serialize!(self.last_checkpoint, destination, _cursor);
        serialize!(self.storage_filename, destination, _cursor);
        serialize!(self.allocator, destination, _cursor);
    }

    fn deserialize(page: Page, fd: File) -> Self {
        let src: &[u8] = (&page).into();
        let mut _cursor = 0;
        deserialize_with_var!(magic, u64, src, _cursor);
        assert_eq!(magic, MAGIC);
        deserialize_with_var!(root, PageId, src, _cursor);
        deserialize_with_var!(last_checkpoint, u64, src, _cursor);
        deserialize_with_var!(storage_filename, String, src, _cursor);
        deserialize_with_var!(allocator, SimpleAllocator, src, _cursor);
        info!("root: {}, Deseri: {:?}", root, allocator);
        deserialize_with_var!(wal, Wal, src, _cursor);
        Self {
            root,
            last_checkpoint,
            last_flushed_root: root,
            storage_filename,
            allocator,
            wal,
            fd,
            page,
        }
    }

    pub fn set_root(&mut self, root: PageId) {
        self.root = root;
    }

    pub fn flush_sb(&mut self) -> Result<(), Error> {
        self.fd.seek(SeekFrom::Start(SB_PAGE_ID * PAGESIZE))?;
        self.serialize();
        self.fd.write_all((&self.page).into()).unwrap();
        // flush != fsync, flush only flushes the data from current process to the kernel
        self.fd.sync_all().unwrap();
        self.last_flushed_root = self.root;
        Ok(())
    }

    // Precondition: node ID always increments
    pub fn safe_to_overwrite_in_place(&self, node: ChildId) -> bool {
        node > self.last_flushed_root
    }

    pub fn flush_wal(&mut self) {}

    fn load_superblock(mut fd: File) -> Superblock {
        fd.seek(SeekFrom::Start(SB_PAGE_ID * PAGESIZE)).unwrap();
        let mut page = Page::default();
        fd.read_exact((&mut page).into()).unwrap();
        Self::deserialize(page, fd)
    }

    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let allocator = SimpleAllocator::default();
        let mut storage_filename = path.as_ref().to_str().unwrap().to_owned();
        storage_filename.push_str(META_EXT);
        let fd = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let wal = Wal::default();
        Self {
            allocator,
            wal,
            root: 0,
            last_flushed_root: 0,
            last_checkpoint: 0,
            storage_filename,
            fd,
            page: Page::default(),
        }
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let fd = OpenOptions::new()
            .create(false)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .unwrap();
        Self::load_superblock(fd)
    }

    pub fn exists<P: AsRef<Path>>(p: &P) -> bool {
        p.as_ref().try_exists().unwrap_or(false)
    }
}
