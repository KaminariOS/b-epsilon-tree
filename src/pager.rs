pub type PageId = u64;
use crate::page::{Page, PAGESIZE};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::error::Error;

pub trait Pager: Sized {
    // const DEFAULT_PATH: &'static str = "/tmp/dbtest";
    fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error>;
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error>;
    fn read(&mut self, page_id: &PageId, page: &mut Page) -> Result<(), Error>;
    fn write(&mut self, page_id: &PageId, data: &Page) -> Result<(), Error>;
    fn flush(&mut self) -> Result<(), Error>;
}

pub struct SimplePager {
    file: File,
}

// impl Default for SimplePager {
//     fn default() -> Self {
//         Self::new(None, None).unwrap()
//     }
// }

impl Pager for SimplePager {
    fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let fd = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        Ok(Self { file: fd })
    }

    fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let fd = OpenOptions::new()
            .create(false)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        Ok(Self { file: fd })
    }

    fn read(&mut self, page_id: &PageId, page: &mut Page) -> Result<(), Error> {
        self.file.seek(SeekFrom::Start(page_id * PAGESIZE))?;
        self.file.read_exact(page.into())?;
        Ok(())
    }
    fn write(&mut self, page_id: &PageId, data: &Page) -> Result<(), Error> {
        self.file.seek(SeekFrom::Start(page_id * PAGESIZE))?;
        self.file.write_all(data.into())?;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Error> {
        self.file.sync_all()?;
        Ok(())
    }
}

#[test]
fn test_persist() {
    let mut pager = if let Ok(p) = SimplePager::open("/tmp/dbtest_p") {
        p
    } else {
        SimplePager::new("/tmp/dbtest_p").unwrap()
    };
    let mut a = Page::default();
    let fill = 9;
    a.fill(fill);
    let page_id = 10;
    pager.write(&page_id, &a).unwrap();
    let mut b = Page::default();
    pager.read(&page_id, &mut b).unwrap();
    let a1: &[u8] = (&a).into();
    let b1: &[u8] = (&b).into();
    assert_eq!(a1, b1);
}
