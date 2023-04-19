#[cfg(test)]
use mockall::automock;

use crate::dbms::storage::page::PAGE_SIZE;

pub type DiskManagerError = ();

#[cfg_attr(test, automock)]
pub trait IDiskManager {
    fn write_page(&mut self, page_id: usize, page: &[u8]) -> Result<(), DiskManagerError>;
    fn read_page(&mut self, page_id: usize) -> Result<[u8; PAGE_SIZE], DiskManagerError>;
    fn write_log(&mut self, log: &[u8]) -> Result<(), DiskManagerError>;
    fn read_log(&mut self, size: usize, offset: usize)
        -> Result<[u8; PAGE_SIZE], DiskManagerError>;
    fn allocate_page(&mut self) -> Result<usize, DiskManagerError>;
    fn deallocate_page(&mut self, page_id: usize) -> Result<(), DiskManagerError>;
}
