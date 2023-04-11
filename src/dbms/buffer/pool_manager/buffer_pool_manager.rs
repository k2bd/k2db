#[cfg(test)]
use mockall::automock;

use std::sync::RwLock;

use crate::dbms::buffer::replacer::IBufferPoolReplacer;
use crate::dbms::storage::disk_manager::IDiskManager;

pub enum BufferPoolManagerError {}

#[cfg_attr(test, automock)]
pub trait IBufferPoolManager {
    fn fetch_page(&self, page_id: usize) -> Result<Vec<u8>, BufferPoolManagerError>;
    fn new_page(&self) -> Result<usize, BufferPoolManagerError>;
    fn unpin_page(&self, page_id: usize) -> Result<(), BufferPoolManagerError>;
    fn flush_page(&self, page_id: usize) -> Result<(), BufferPoolManagerError>;
    fn delete_page(&self, page_id: usize) -> Result<(), BufferPoolManagerError>;
    fn flush_all_pages(&self) -> Result<(), BufferPoolManagerError>;
}

struct BufferPoolManager {
    replacer: RwLock<Box<dyn IBufferPoolReplacer>>,
    disk_manager: RwLock<Box<dyn IDiskManager>>,
}
