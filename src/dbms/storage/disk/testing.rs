use std::collections::HashMap;

use crate::dbms::types::{PageData, PAGE_SIZE, PageId};

use super::{DiskManagerError, IDiskManager};

/// A purely in-memory implementation of the DiskManager trait for testing purposes.
/// Also exposes the underlying data structures for inspection in tests.
pub struct InMemoryDiskManager {
    /// page_id -> page_data
    pub pages: HashMap<PageId, Vec<u8>>,
    /// log_id -> log_data
    pub logs: HashMap<usize, Vec<u8>>,
    pub next_page_id: PageId,
}

impl InMemoryDiskManager {
    #[cfg(test)]
    pub fn new() -> Self {
        Self {
            pages: HashMap::new(),
            logs: HashMap::new(),
            next_page_id: 0,
        }
    }
}

impl IDiskManager for InMemoryDiskManager {
    fn write_page(&mut self, page_id: PageId, page: &[u8]) -> Result<(), DiskManagerError> {
        // Must allocate page before writing to it
        if !self.pages.contains_key(&page_id) {
            return Err(DiskManagerError::PageNotFound);
        }
        self.pages.insert(page_id, page.to_vec());
        Ok(())
    }

    fn read_page(&self, page_id: PageId) -> Result<PageData, DiskManagerError> {
        let page = match self.pages.get(&page_id) {
            Some(page) => page,
            None => return Err(DiskManagerError::PageNotFound),
        };
        let mut page_data = [0u8; PAGE_SIZE];
        page_data.copy_from_slice(page);
        Ok(page_data)
    }

    fn write_log(&mut self, _log: &[u8]) -> Result<(), DiskManagerError> {
        todo!()
    }

    fn read_log(&self, _size: usize, _offset: usize) -> Result<PageData, DiskManagerError> {
        todo!()
    }

    fn allocate_page(&mut self) -> Result<PageId, DiskManagerError> {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        self.pages.insert(page_id, vec![0u8; PAGE_SIZE]);
        Ok(page_id)
    }

    fn deallocate_page(&mut self, page_id: PageId) -> Result<(), DiskManagerError> {
        self.pages.remove(&page_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    fn test_write_page() {
        let mut disk_manager = InMemoryDiskManager::new();
        let page_id = disk_manager.allocate_page().unwrap();
        let page = [1u8; PAGE_SIZE];
        disk_manager.write_page(page_id, &page).unwrap();
        assert_eq!(disk_manager.pages.get(&page_id).unwrap(), &page);
    }

    #[rstest]
    fn test_write_page_nonexistent() {
        let mut disk_manager = InMemoryDiskManager::new();
        let page_id = disk_manager.allocate_page().unwrap();
        let page = [1u8; PAGE_SIZE];
        let result = disk_manager.write_page(page_id + 1, &page);
        assert!(result.is_err());
    }

    #[rstest]
    fn test_read_page() {
        let mut disk_manager = InMemoryDiskManager::new();
        let page_id = disk_manager.allocate_page().unwrap();
        let page = [1u8; PAGE_SIZE];
        disk_manager.write_page(page_id, &page).unwrap();
        let read_page = disk_manager.read_page(page_id).unwrap();
        assert_eq!(read_page, page);
    }

    #[rstest]
    fn test_read_page_nonexistent() {
        let mut disk_manager = InMemoryDiskManager::new();
        let page_id = disk_manager.allocate_page().unwrap();
        let result = disk_manager.read_page(page_id + 1);
        assert!(result.is_err());
    }

    #[rstest]
    fn test_allocate_page() {
        let mut disk_manager = InMemoryDiskManager::new();
        let page_id = disk_manager.allocate_page().unwrap();
        assert_eq!(page_id, 0);
        assert_eq!(disk_manager.pages.len(), 1);
        let page_id = disk_manager.allocate_page().unwrap();
        assert_eq!(page_id, 1);
        assert_eq!(disk_manager.pages.len(), 2);
    }

    #[rstest]
    fn test_deallocate_page() {
        let mut disk_manager = InMemoryDiskManager::new();
        disk_manager.allocate_page().unwrap();
        disk_manager.allocate_page().unwrap();
        disk_manager.allocate_page().unwrap();
        assert_eq!(disk_manager.pages.len(), 3);
        disk_manager.deallocate_page(1).unwrap();
        assert_eq!(disk_manager.pages.len(), 2);
        assert!(disk_manager.pages.get(&1).is_none());
    }

    #[rstest]
    fn test_deallocate_page_non_existent() {
        let mut disk_manager = InMemoryDiskManager::new();
        disk_manager.allocate_page().unwrap();
        disk_manager.allocate_page().unwrap();
        disk_manager.allocate_page().unwrap();
        assert_eq!(disk_manager.pages.len(), 3);
        disk_manager.deallocate_page(3).unwrap();
        assert_eq!(disk_manager.pages.len(), 3);
    }

    #[rstest]
    fn test_deallocated_page_ids_not_reused() {
        let mut disk_manager = InMemoryDiskManager::new();
        disk_manager.allocate_page().unwrap();
        disk_manager.allocate_page().unwrap();
        disk_manager.allocate_page().unwrap();
        assert_eq!(disk_manager.pages.len(), 3);
        disk_manager.deallocate_page(1).unwrap();
        assert_eq!(disk_manager.pages.len(), 2);
        assert!(disk_manager.pages.get(&3).is_none());
        let page_id = disk_manager.allocate_page().unwrap();
        assert_eq!(page_id, 3);
        assert_eq!(disk_manager.pages.len(), 3);
        assert!(disk_manager.pages.get(&3).is_some());
        assert!(disk_manager.pages.get(&1).is_none());
    }
}
