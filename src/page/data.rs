use crate::page::header::PAGE_HEADER_SIZE;
use crate::page::page::Page;
use crate::page::{GeneralHeader, PageId, PAGE_SIZE};

pub const DATA_PAGE_HEADER_SIZE: usize = size_of::<DataPageHeader>();
pub const DATA_PAGE_BODY_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE - DATA_PAGE_HEADER_SIZE;
#[repr(align(8))]
pub struct DataPageHeader {
    pub offset: u32,
}
pub struct DataPage {
    pub page: Page,
}
impl DataPage {
    pub fn new() -> Self {
        Self {
            page: Page::default(),
        }
    }
    pub fn page_id(&self) -> PageId {
        self.page.header().page_id
    }
    pub fn header(&self) -> &GeneralHeader {
        self.page.header()
    }
    pub fn header_mut(&mut self) -> &mut GeneralHeader {
        self.page.header_mut()
    }
    pub fn data_header(&self) -> &DataPageHeader {
        unsafe { std::mem::transmute(self.page.body().as_ptr()) }
    }
    pub fn data_header_mut(&mut self) -> &mut DataPageHeader {
        unsafe { std::mem::transmute(self.page.body_mut().as_mut_ptr()) }
    }
    pub fn body(&self) -> &[u8] {
        &self.page.body()[DATA_PAGE_HEADER_SIZE..]
    }
    pub fn body_mut(&mut self) -> &mut [u8] {
        &mut self.page.body_mut()[DATA_PAGE_HEADER_SIZE..]
    }
}
