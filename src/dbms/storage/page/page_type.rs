#[cfg(test)]
use mockall::automock;

use std::sync::{RwLock, RwLockReadGuard};

const PAGE_SIZE: usize = 4096;

#[cfg_attr(test, automock)]
pub trait IPage {
    /// Get a readable copy of the page's data
    fn get_data(&self) -> RwLockReadGuard<'_, [u8; PAGE_SIZE]>;
    /// Set the whole content of the page, and set the page to dirty
    fn set_data(&mut self, data: [u8; PAGE_SIZE]);
    /// Write a slice of the page, starting from the given offset, and set the
    /// page to dirty
    fn write_data(&mut self, offset: usize, data: &[u8]);
    /// Get the page ID
    fn get_page_id(&self) -> usize;
    /// Get whether the page is dirty
    fn is_dirty(&self) -> RwLockReadGuard<'_, bool>;
    /// Set the page to dirty
    fn set_dirty(&mut self);
    /// Set the page to clean
    fn set_clean(&mut self);
    /// Increase the pin count of the page by 1
    fn increase_pin_count(&mut self);
    /// Get the pin count of the page
    fn get_pin_count(&self) -> RwLockReadGuard<'_, usize>;
}

pub struct Page {
    data: RwLock<[u8; PAGE_SIZE]>,
    page_id: usize,
    pin_count: RwLock<usize>,
    is_dirty: RwLock<bool>,
}

impl Page {
    pub fn new(page_id: usize) -> Page {
        Page {
            data: RwLock::new([0; PAGE_SIZE]),
            page_id,
            pin_count: RwLock::new(0),
            is_dirty: RwLock::new(false),
        }
    }
}

impl IPage for Page {
    fn get_data(&self) -> RwLockReadGuard<'_, [u8; PAGE_SIZE]> {
        let data = self.data.read().unwrap();
        data
    }

    fn set_data(&mut self, data: [u8; PAGE_SIZE]) {
        self.write_data(0, &data)
    }

    fn write_data(&mut self, offset: usize, data: &[u8]) {
        let mut page_data = self.data.write().unwrap();
        let mut is_dirty = self.is_dirty.write().unwrap();
        *is_dirty = true;
        page_data[offset..offset + data.len()].copy_from_slice(data);
    }

    fn get_page_id(&self) -> usize {
        self.page_id
    }

    fn is_dirty(&self) -> RwLockReadGuard<'_, bool> {
        let is_dirty = self.is_dirty.read().unwrap();
        is_dirty
    }

    fn increase_pin_count(&mut self) {
        let mut pin_count = self.pin_count.write().unwrap();
        *pin_count += 1;
    }

    fn get_pin_count(&self) -> RwLockReadGuard<'_, usize> {
        let pin_count = self.pin_count.read().unwrap();
        pin_count
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


#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_set_and_get_data() {
        let mut page = Page::new(0);
        assert_eq!(page.is_dirty().to_owned(), false);

        let new_data = [1; PAGE_SIZE];
        page.set_data(new_data);

        let data = page.get_data();

        assert_eq!(data.len(), PAGE_SIZE);
        assert_eq!(data[..], new_data);
        assert_eq!(page.is_dirty().to_owned(), true);
    }

    #[rstest]
    fn test_write_data() {
        let mut page = Page::new(0);
        assert_eq!(page.is_dirty().to_owned(), false);

        let new_data = [1; 16];
        page.write_data(32, &new_data);

        let data = page.get_data();

        assert_eq!(data.len(), PAGE_SIZE);
        assert_eq!(data[0..32], [0; 32]);
        assert_eq!(data[32..48], new_data);
        assert_eq!(data[48..PAGE_SIZE], [0; PAGE_SIZE - 48]);
        assert_eq!(page.is_dirty().to_owned(), true);
    }

    #[rstest]
    fn test_get_page_id() {
        let page = Page::new(123);
        assert_eq!(page.get_page_id(), 123);
    }

    #[rstest]
    fn test_set_dirty_clean() {
        let mut page = Page::new(0);
        assert_eq!(page.is_dirty().to_owned(), false);
        page.set_dirty();
        assert_eq!(page.is_dirty().to_owned(), true);
        page.set_clean();
        assert_eq!(page.is_dirty().to_owned(), false);
    }

    #[rstest]
    fn test_increase_pin_count() {
        let mut page = Page::new(0);
        assert_eq!(page.get_pin_count().to_owned(), 0);
        page.increase_pin_count();
        assert_eq!(page.get_pin_count().to_owned(), 1);
        page.increase_pin_count();
        assert_eq!(page.get_pin_count().to_owned(), 2);
    }
}
