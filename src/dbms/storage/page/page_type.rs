#[cfg(test)]
use mockall::automock;

use std::sync::RwLock;

const PAGE_SIZE: usize = 4096;

#[cfg_attr(test, automock)]
pub trait IPage {
    /// Get the whole contents of the page
    fn get_data(&self) -> [u8; PAGE_SIZE];
    /// Set the whole content of the page, and set the page to dirty
    fn set_data(&mut self, data: [u8; PAGE_SIZE]);
    /// Read a slice of the page, starting from the given offset
    fn read_data(&self, offset: usize, size: usize) -> Vec<u8>;
    /// Write a slice of the page, starting from the given offset, and set the
    /// page to dirty
    fn write_data(&mut self, offset: usize, data: &[u8]);
    /// Get the page ID
    fn get_page_id(&self) -> usize;
    /// Get whether the page is dirty
    fn is_dirty(&self) -> bool;
    /// Set the page to dirty
    fn set_dirty(&mut self);
    /// Set the page to clean
    fn set_clean(&mut self);
    /// Increase the pin count of the page by 1
    fn increase_pin_count(&mut self);
    /// Get the pin count of the page
    fn get_pin_count(&self) -> usize;
}

struct Page {
    data: RwLock<[u8; PAGE_SIZE]>,
    page_id: RwLock<usize>,
    pin_count: RwLock<usize>,
    is_dirty: RwLock<bool>,
}

impl Page {
    fn new(page_id: usize) -> Page {
        Page {
            data: RwLock::new([0; PAGE_SIZE]),
            page_id: RwLock::new(page_id),
            pin_count: RwLock::new(0),
            is_dirty: RwLock::new(false),
        }
    }
}

impl IPage for Page {
    fn get_data(&self) -> [u8; PAGE_SIZE] {
        let data = self.data.read().unwrap();
        *data
    }

    fn set_data(&mut self, data: [u8; PAGE_SIZE]) {
        let mut page_data = self.data.write().unwrap();
        let mut is_dirty = self.is_dirty.write().unwrap();
        *is_dirty = true;
        *page_data = data;
    }

    fn read_data(&self, offset: usize, size: usize) -> Vec<u8> {
        let data = self.data.read().unwrap();
        data[offset..offset + size].to_vec()
    }

    fn write_data(&mut self, offset: usize, data: &[u8]) {
        let mut page_data = self.data.write().unwrap();
        let mut is_dirty = self.is_dirty.write().unwrap();
        *is_dirty = true;
        page_data[offset..offset + data.len()].copy_from_slice(data);
    }

    fn get_page_id(&self) -> usize {
        let page_id = self.page_id.read().unwrap();
        *page_id
    }

    fn is_dirty(&self) -> bool {
        let is_dirty = self.is_dirty.read().unwrap();
        *is_dirty
    }

    fn increase_pin_count(&mut self) {
        let mut pin_count = self.pin_count.write().unwrap();
        *pin_count += 1;
    }

    fn get_pin_count(&self) -> usize {
        let pin_count = self.pin_count.read().unwrap();
        *pin_count
    }

    fn set_dirty(&mut self) {
        let mut is_dirty = self.is_dirty.write().unwrap();
        *is_dirty = true;
    }

    fn set_clean(&mut self) {
        let mut is_dirty = self.is_dirty.write().unwrap();
        *is_dirty = false;
    }
}
