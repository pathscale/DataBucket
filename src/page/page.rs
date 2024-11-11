use crate::page::header::GeneralHeader;
use crate::page::PAGE_SIZE;
use std::intrinsics::transmute;

#[derive(PartialEq)]
#[repr(align(8))]
pub struct Page {
    pub raw_data: [u8; PAGE_SIZE],
}
impl Page {
    pub fn from_bytes(bytes: &[u8]) -> &Self {
        assert_eq!(bytes.len(), PAGE_SIZE);
        unsafe { transmute(bytes.as_ptr()) }
    }
    pub fn from_bytes_mut(bytes: &mut [u8]) -> &mut Self {
        assert_eq!(bytes.len(), PAGE_SIZE);
        unsafe { transmute(bytes.as_ptr()) }
    }
    pub fn header(&self) -> &GeneralHeader {
        unsafe { transmute(&self.raw_data) }
    }
    pub fn header_mut(&mut self) -> &mut GeneralHeader {
        unsafe { transmute(&mut self.raw_data) }
    }
    pub fn body(&self) -> &[u8] {
        &self.raw_data[size_of::<GeneralHeader>()..]
    }
    pub fn body_mut(&mut self) -> &mut [u8] {
        &mut self.raw_data[size_of::<GeneralHeader>()..]
    }
}
impl Default for Page {
    fn default() -> Self {
        Self {
            raw_data: [0; PAGE_SIZE],
        }
    }
}
