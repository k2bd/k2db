use std::collections::HashMap;

use crate::dbms::storage::page::{PageData, PAGE_SIZE};

use super::{DiskManagerError, IDiskManager};

/// A purely in-memory implementation of the DiskManager trait for testing purposes.
/// Also exposes the underlying data structures for inspection in tests.
pub struct InMemoryDiskManager {
    /// page_id -> page_data
    pub pages: HashMap<usize, Vec<u8>>,
    /// log_id -> log_data
    pub logs: HashMap<usize, Vec<u8>>,
}

impl InMemoryDiskManager {
    pub fn new() -> Self {
        Self {
            pages: HashMap::new(),
            logs: HashMap::new(),
        }
    }
}

impl IDiskManager for InMemoryDiskManager {
    fn write_page(&mut self, page_id: usize, page: &[u8]) -> Result<(), DiskManagerError> {
        self.pages.insert(page_id, page.to_vec());
        Ok(())
    }

    fn read_page(&self, page_id: usize) -> Result<PageData, DiskManagerError> {
        let page = match self.pages.get(&page_id) {
            Some(page) => page,
            None => return Err(DiskManagerError::PageNotFound),
        };
        let mut page_data = [0u8; PAGE_SIZE];
        page_data.copy_from_slice(page);
        Ok(page_data)
    }

    fn write_log(&mut self, log: &[u8]) -> Result<(), DiskManagerError> {
        let log_id = self.logs.len();
        self.logs.insert(log_id, log.to_vec());
        Ok(())
    }

    fn read_log(&self, size: usize, offset: usize) -> Result<PageData, DiskManagerError> {
        let mut log_data = [0u8; PAGE_SIZE];
        let log = match self.logs.get(&offset) {
            Some(l) => l,
            None => return Err(DiskManagerError::PageNotFound),
        };
        log_data[..size].copy_from_slice(log);
        Ok(log_data)
    }

    fn allocate_page(&mut self) -> Result<usize, DiskManagerError> {
        let page_id = self.pages.len();
        self.pages.insert(page_id, vec![0u8; PAGE_SIZE]);
        Ok(page_id)
    }

    fn deallocate_page(&mut self, page_id: usize) -> Result<(), DiskManagerError> {
        self.pages.remove(&page_id);
        Ok(())
    }
}
