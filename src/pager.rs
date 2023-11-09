pub type PageId = u64;
use crate::page::{Page, PAGESIZE};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::io::{Read, Seek, SeekFrom};

use crate::error::Error;

pub trait Pager: Sized {
    const DEFAULT_PATH: &'static str = "/tmp/dbtest";
    fn new(path: Option<String>, cursor: Option<PageId>) -> Result<Self, Error>;
    fn read(&mut self, page_id: PageId, page: &mut Page) -> Result<(), Error>;
    fn write(&mut self, page_id: PageId, data: &Page) -> Result<(), Error>;
    fn flush(&mut self) -> Result<(), Error>;
}

pub struct SimplePager {
    file: File,
}

impl Default for SimplePager {
    fn default() -> Self {
        Self::new(None, None).unwrap()
    }
}

impl Pager for SimplePager {
    fn new(path: Option<String>, cursor: Option<PageId>) -> Result<Self, Error> {
        let path_str = path
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or(Self::DEFAULT_PATH);
        let fd = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(!cursor.is_some())
            .open(path_str)?;
        Ok(Self {
            file: fd,
        })
    }

    fn read(&mut self, page_id: PageId, page: &mut Page) -> Result<(), Error> {
        self.file.seek(SeekFrom::Start(page_id * PAGESIZE))?;
        self.file.read_exact(page.into())?;
        Ok(())
    }
    fn write(&mut self, page_id: PageId, data: &Page) -> Result<(), Error> {
        self.file.seek(SeekFrom::Start(page_id * PAGESIZE))?;
        self.file.write_all(data.into())?;
        Ok(())
    }
    fn flush(&mut self) -> Result<(), Error> {
        self.file.flush()?;
        Ok(())
    }
}

#[test]
fn test_persist() {
    let mut pager = SimplePager::default();
    let mut a = Page::default();
    let fill = 9;
    a.fill(fill);
    let page_id = 10;
    pager.write(page_id, &a).unwrap();
    let mut b = Page::default();
    pager.read(page_id, &mut b).unwrap();
    let a1: &[u8] = (&a).into();
    let b1: &[u8] = (&b).into();
    assert_eq!(a1, b1);
}
