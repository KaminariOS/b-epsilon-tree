use core::mem;
use core::ops::{Deref, DerefMut};
pub const PAGESIZE: u64 = 4096;

pub type PageType = [u8; PAGESIZE as usize];

#[repr(C, align(4096))]
#[derive(Clone, Debug)]
struct PageRaw(PageType);

impl Default for PageRaw {
    fn default() -> Self {
        Self([0; PAGESIZE as usize])
    }
}

#[derive(Clone, Debug, Default)]
pub struct Page(Box<PageRaw>);

impl Deref for Page {
    type Target = PageType;
    fn deref(&self) -> &Self::Target {
        &self.0.0
    }
}

impl DerefMut for Page {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.0    
    }
}

impl<'a> From<&'a Page> for &'a [u8] {
    fn from(value: &'a Page) -> Self {
        &value.0.0
    }
}


impl<'a> From<&'a mut Page> for &'a mut [u8] {
    fn from(value: &'a mut Page) -> Self {
        &mut value.0.0
    }
}


#[test]
fn test_layout() {
    assert_eq!(PAGESIZE as usize, mem::size_of::<PageRaw>());
    assert_eq!(PAGESIZE as usize, mem::align_of::<PageRaw>());
}
