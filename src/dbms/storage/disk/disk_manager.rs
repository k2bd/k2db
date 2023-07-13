use crate::dbms::types::{PageData, PageId};

#[derive(Debug, PartialEq, Eq)]
pub enum DiskManagerError {
    PageNotFound,
    PageIdOverflow,
}

pub trait IDiskManager {
    fn write_page(&mut self, page_id: PageId, page: &[u8]) -> Result<(), DiskManagerError>;
    fn read_page(&self, page_id: PageId) -> Result<PageData, DiskManagerError>;
    fn write_log(&mut self, log: &[u8]) -> Result<(), DiskManagerError>;
    fn read_log(&self, size: usize, offset: usize) -> Result<PageData, DiskManagerError>;
    fn allocate_page(&mut self) -> Result<PageId, DiskManagerError>;
    fn deallocate_page(&mut self, page_id: PageId) -> Result<(), DiskManagerError>;
}
