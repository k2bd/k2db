#[cfg(test)]
use mockall::automock;

use std::sync::{Arc, RwLock};

pub const PAGE_SIZE: usize = 4096;

pub type PageData = [u8; PAGE_SIZE];

#[derive(Debug, PartialEq)]
pub enum PageError {
    LockPoisoned(String),
}

#[cfg_attr(test, automock)]
pub trait IPage {
    /// Get a thread-safe read-only version of the page's data
    fn get_data(&self) -> Result<PageData, PageError>;
    /// Set the whole content of the page, and set the page to dirty
    fn set_data(&mut self, data: PageData) -> Result<(), PageError>;
    /// Write a slice of the page, starting from the given offset, and set the
    /// page to dirty
    fn write_data(&mut self, offset: usize, data: &[u8]) -> Result<(), PageError>;
    /// Get the page ID
    fn get_page_id(&self) -> Result<usize, PageError>;
    /// Get whether the page is dirty
    fn is_dirty(&self) -> Result<bool, PageError>;
    /// Set the page to dirty
    fn set_dirty(&mut self) -> Result<(), PageError>;
    /// Set the page to clean
    fn set_clean(&mut self) -> Result<(), PageError>;
    /// Increase the pin count of the page by 1
    fn increase_pin_count(&mut self) -> Result<(), PageError>;
    /// Get the pin count of the page
    fn get_pin_count(&self) -> Result<usize, PageError>;
    /// Full overwrite page, e.g. when a new page is fetched from disk
    fn overwrite(&mut self, page_id: usize, data: PageData) -> Result<(), PageError>;
}

#[derive(Clone)]
pub struct Page {
    data: Arc<RwLock<PageData>>,
    page_id: Arc<RwLock<usize>>,
    pin_count: Arc<RwLock<usize>>,
    is_dirty: Arc<RwLock<bool>>,
}

impl Page {
    pub fn new(page_id: usize) -> Page {
        Page {
            data: Arc::new(RwLock::new([0; PAGE_SIZE])),
            page_id: Arc::new(RwLock::new(page_id)),
            pin_count: Arc::new(RwLock::new(0)),
            is_dirty: Arc::new(RwLock::new(false)),
        }
    }
}

impl IPage for Page {
    fn get_data(&self) -> Result<PageData, PageError> {
        let page_data_r = self.data.read();

        match page_data_r {
            Ok(page_data) => Ok(*page_data),
            Err(_) => Err(PageError::LockPoisoned(
                "Page data lock poisoned".to_string(),
            )),
        }
    }

    fn set_data(&mut self, data: PageData) -> Result<(), PageError> {
        self.write_data(0, &data)
    }

    fn write_data(&mut self, offset: usize, data: &[u8]) -> Result<(), PageError> {
        let mut page_data_r = self.data.write();
        let mut is_dirty_r = self.is_dirty.write();

        match (page_data_r, is_dirty_r) {
            (Ok(mut page_data), Ok(mut is_dirty)) => {
                *is_dirty = true;
                page_data[offset..offset + data.len()].copy_from_slice(data);
                Ok(())
            }
            _ => Err(PageError::LockPoisoned(
                "Page data lock poisoned".to_string(),
            )),
        }
    }

    fn get_page_id(&self) -> Result<usize, PageError> {
        let page_id_r = self.page_id.read();

        match page_id_r {
            Ok(page_id) => Ok(*page_id),
            Err(_) => Err(PageError::LockPoisoned("Page ID lock poisoned".to_string())),
        }
    }

    fn is_dirty(&self) -> Result<bool, PageError> {
        let is_dirty_r = self.is_dirty.read();

        match is_dirty_r {
            Ok(is_dirty) => Ok(*is_dirty),
            Err(_) => Err(PageError::LockPoisoned(
                "Page dirty lock poisoned".to_string(),
            )),
        }
    }

    fn increase_pin_count(&mut self) -> Result<(), PageError> {
        let mut pin_count_r = self.pin_count.write();

        match pin_count_r {
            Ok(mut pin_count) => {
                *pin_count += 1;
                Ok(())
            }
            Err(_) => Err(PageError::LockPoisoned(
                "Page pin count lock poisoned".to_string(),
            )),
        }
    }

    fn get_pin_count(&self) -> Result<usize, PageError> {
        let pin_count_r = self.pin_count.read();

        match pin_count_r {
            Ok(pin_count) => Ok(*pin_count),
            Err(_) => Err(PageError::LockPoisoned(
                "Page pin count lock poisoned".to_string(),
            )),
        }
    }

    fn set_dirty(&mut self) -> Result<(), PageError> {
        let mut is_dirty = self.is_dirty.write();

        match is_dirty {
            Ok(mut is_dirty) => {
                *is_dirty = true;
                Ok(())
            }
            Err(_) => Err(PageError::LockPoisoned(
                "Page dirty lock poisoned".to_string(),
            )),
        }
    }

    fn set_clean(&mut self) -> Result<(), PageError> {
        let mut is_dirty = self.is_dirty.write();

        match is_dirty {
            Ok(mut is_dirty) => {
                *is_dirty = false;
                Ok(())
            }
            Err(_) => Err(PageError::LockPoisoned(
                "Page dirty lock poisoned".to_string(),
            )),
        }
    }

    fn overwrite(&mut self, page_id: usize, data: PageData) -> Result<(), PageError> {
        let mut page_id_writer = self.page_id.write();
        let mut page_data_writer = self.data.write();
        let mut is_dirty_writer = self.is_dirty.write();
        let mut pin_count_writer = self.pin_count.write();

        match (
            page_id_writer,
            page_data_writer,
            is_dirty_writer,
            pin_count_writer,
        ) {
            (Ok(mut page_id_w), Ok(mut page_data), Ok(mut is_dirty), Ok(mut pin_count)) => {
                *page_id_w = page_id;
                *page_data = data;
                *is_dirty = false;
                *pin_count = 0;
                Ok(())
            }
            _ => Err(PageError::LockPoisoned("Page lock poisoned".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_set_and_get_data() {
        let mut page = Page::new(0);
        assert_eq!(page.is_dirty().unwrap(), false);

        let new_data = [1; PAGE_SIZE];
        page.set_data(new_data);

        let data = page.get_data().unwrap();

        assert_eq!(data.len(), PAGE_SIZE);
        assert_eq!(data[..], new_data);
        assert_eq!(page.is_dirty().unwrap(), true);
    }

    #[rstest]
    fn test_write_data() {
        let mut page = Page::new(0);
        assert_eq!(page.is_dirty().unwrap(), false);

        let new_data = [1; 16];
        page.write_data(32, &new_data);

        let data = page.get_data().unwrap();

        assert_eq!(data.len(), PAGE_SIZE);
        assert_eq!(data[0..32], [0; 32]);
        assert_eq!(data[32..48], new_data);
        assert_eq!(data[48..PAGE_SIZE], [0; PAGE_SIZE - 48]);
        assert_eq!(page.is_dirty().unwrap(), true);
    }

    #[rstest]
    fn test_get_page_id() {
        let page = Page::new(123);
        assert_eq!(page.get_page_id().unwrap(), 123);
    }

    #[rstest]
    fn test_set_dirty_clean() {
        let mut page = Page::new(0);
        assert_eq!(page.is_dirty().unwrap(), false);
        page.set_dirty();
        assert_eq!(page.is_dirty().unwrap(), true);
        page.set_clean();
        assert_eq!(page.is_dirty().unwrap(), false);
    }

    #[rstest]
    fn test_increase_pin_count() {
        let mut page = Page::new(0);
        assert_eq!(page.get_pin_count().unwrap(), 0);
        page.increase_pin_count();
        assert_eq!(page.get_pin_count().unwrap(), 1);
        page.increase_pin_count();
        assert_eq!(page.get_pin_count().unwrap(), 2);
    }
}
