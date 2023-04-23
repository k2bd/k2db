use crate::dbms::storage::page::PageData;

#[derive(Debug)]
pub enum DiskManagerError {
    PageNotFound,
}

pub trait IDiskManager {
    fn write_page(&mut self, page_id: usize, page: &[u8]) -> Result<(), DiskManagerError>;
    fn read_page(&self, page_id: usize) -> Result<PageData, DiskManagerError>;
    fn write_log(&mut self, log: &[u8]) -> Result<(), DiskManagerError>;
    fn read_log(&self, size: usize, offset: usize) -> Result<PageData, DiskManagerError>;
    fn allocate_page(&mut self) -> Result<usize, DiskManagerError>;
    fn deallocate_page(&mut self, page_id: usize) -> Result<(), DiskManagerError>;
}
