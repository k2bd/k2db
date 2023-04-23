pub const PAGE_SIZE: usize = 4096;

pub type PageData = [u8; PAGE_SIZE];

pub type PageError = ();

pub trait IPage {
    /// Get a copy of the page's data
    fn get_data(&self) -> Result<PageData, PageError>;
    /// Set the whole content of the page, and set the page to dirty
    fn set_data(&mut self, data: PageData) -> Result<(), PageError>;
    /// Write a slice of the page, starting from the given offset, and set the
    /// page to dirty
    fn write_data(&mut self, offset: usize, data: &[u8]) -> Result<(), PageError>;
    /// Get the page ID
    fn get_page_id(&self) -> Result<Option<usize>, PageError>;
    /// Get whether the page is dirty
    fn is_dirty(&self) -> Result<bool, PageError>;
    /// Set the page to dirty
    fn set_dirty(&mut self) -> Result<(), PageError>;
    /// Set the page to clean
    fn set_clean(&mut self) -> Result<(), PageError>;
    /// Increase the pin count of the page by 1
    fn increase_pin_count(&mut self) -> Result<(), PageError>;
    /// Decrease the pin count of the page by 1
    fn decrease_pin_count(&mut self) -> Result<(), PageError>;
    /// Get the pin count of the page
    fn get_pin_count(&self) -> Result<usize, PageError>;
    /// Clear the page, e.g. when initialized as new
    fn clear(&mut self) -> Result<(), PageError>;
    /// Full overwrite page, e.g. when a page is fetched from disk
    fn overwrite(&mut self, page_id: Option<usize>, data: PageData) -> Result<(), PageError>;
}

#[derive(Clone)]
pub struct Page {
    data: PageData,
    page_id: Option<usize>,
    pin_count: usize,
    is_dirty: bool,
}

impl Page {
    pub fn new(page_id: Option<usize>) -> Page {
        Page {
            data: [0; PAGE_SIZE],
            page_id,
            pin_count: 0,
            is_dirty: false,
        }
    }
}

impl IPage for Page {
    fn get_data(&self) -> Result<PageData, PageError> {
        Ok(self.data.clone())
    }

    fn set_data(&mut self, data: PageData) -> Result<(), PageError> {
        self.data = data;
        self.is_dirty = true;
        Ok(())
    }

    fn write_data(&mut self, offset: usize, data: &[u8]) -> Result<(), PageError> {
        self.data[offset..offset + data.len()].copy_from_slice(data);
        self.is_dirty = true;
        Ok(())
    }

    fn get_page_id(&self) -> Result<Option<usize>, PageError> {
        Ok(self.page_id)
    }

    fn is_dirty(&self) -> Result<bool, PageError> {
        Ok(self.is_dirty)
    }

    fn increase_pin_count(&mut self) -> Result<(), PageError> {
        self.pin_count += 1;
        Ok(())
    }

    fn decrease_pin_count(&mut self) -> Result<(), PageError> {
        if self.pin_count > 0 {
            self.pin_count -= 1;
        }
        Ok(())
    }

    fn get_pin_count(&self) -> Result<usize, PageError> {
        Ok(self.pin_count)
    }

    fn set_dirty(&mut self) -> Result<(), PageError> {
        self.is_dirty = true;
        Ok(())
    }

    fn set_clean(&mut self) -> Result<(), PageError> {
        self.is_dirty = false;
        Ok(())
    }

    fn clear(&mut self) -> Result<(), PageError> {
        self.page_id = None;
        self.data = [0; PAGE_SIZE];
        self.pin_count = 0;
        self.is_dirty = false;
        Ok(())
    }

    fn overwrite(&mut self, page_id: Option<usize>, data: PageData) -> Result<(), PageError> {
        self.page_id = page_id;
        self.data = data;
        self.pin_count = 0;
        self.is_dirty = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_set_and_get_data() {
        let mut page = Page::new(Some(0));
        assert_eq!(page.is_dirty().unwrap(), false);

        let new_data = [1; PAGE_SIZE];
        let res = page.set_data(new_data);
        assert_eq!(res, Ok(()));

        let data = page.get_data().unwrap();

        assert_eq!(data.len(), PAGE_SIZE);
        assert_eq!(data[..], new_data);
        assert_eq!(page.is_dirty().unwrap(), true);
    }

    #[rstest]
    fn test_write_data() {
        let mut page = Page::new(Some(0));
        assert_eq!(page.is_dirty().unwrap(), false);

        let new_data = [1; 16];
        let res = page.write_data(32, &new_data);
        assert_eq!(res, Ok(()));

        let data = page.get_data().unwrap();

        assert_eq!(data.len(), PAGE_SIZE);
        assert_eq!(data[0..32], [0; 32]);
        assert_eq!(data[32..48], new_data);
        assert_eq!(data[48..PAGE_SIZE], [0; PAGE_SIZE - 48]);
        assert_eq!(page.is_dirty().unwrap(), true);
    }

    #[rstest]
    fn test_get_page_id() {
        let page = Page::new(Some(123));
        assert_eq!(page.get_page_id().unwrap(), Some(123));
    }

    #[rstest]
    fn test_set_dirty_clean() {
        let mut page = Page::new(Some(0));
        assert_eq!(page.is_dirty().unwrap(), false);
        let res1 = page.set_dirty();
        assert_eq!(res1, Ok(()));
        assert_eq!(page.is_dirty().unwrap(), true);
        let res2 = page.set_clean();
        assert_eq!(res2, Ok(()));
        assert_eq!(page.is_dirty().unwrap(), false);
    }

    #[rstest]
    fn test_increase_pin_count() {
        let mut page = Page::new(Some(0));
        assert_eq!(page.get_pin_count().unwrap(), 0);
        let res1 = page.increase_pin_count();
        assert_eq!(res1, Ok(()));
        assert_eq!(page.get_pin_count().unwrap(), 1);
        let res2 = page.increase_pin_count();
        assert_eq!(res2, Ok(()));
        assert_eq!(page.get_pin_count().unwrap(), 2);
    }

    #[rstest]
    fn test_decrease_pin_count() {
        let mut page = Page::new(Some(0));
        let _ = page.increase_pin_count();
        let _ = page.increase_pin_count();

        let res1 = page.decrease_pin_count();
        assert_eq!(res1, Ok(()));
        assert_eq!(page.get_pin_count().unwrap(), 1);
        let res2 = page.decrease_pin_count();
        assert_eq!(res2, Ok(()));
        assert_eq!(page.get_pin_count().unwrap(), 0);
        let res3 = page.decrease_pin_count();
        assert_eq!(res3, Ok(()));
        assert_eq!(page.get_pin_count().unwrap(), 0);
    }

    #[rstest]
    fn test_overwrite() {
        let mut page = Page::new(Some(0));
        let _ = page.increase_pin_count();
        let _ = page.increase_pin_count();
        let _ = page.set_dirty();

        let new_data = [1; PAGE_SIZE];
        let res = page.overwrite(Some(123), new_data);
        assert_eq!(res, Ok(()));

        assert_eq!(page.get_page_id().unwrap(), Some(123));
        assert_eq!(page.get_pin_count().unwrap(), 0);
        assert_eq!(page.is_dirty().unwrap(), false);
        assert_eq!(page.get_data().unwrap(), new_data);
    }

    #[rstest]
    fn test_clear() {
        let mut page = Page::new(Some(0));
        let _ = page.increase_pin_count();
        let _ = page.increase_pin_count();
        let _ = page.set_dirty();

        let res = page.clear();
        assert_eq!(res, Ok(()));

        assert_eq!(page.get_page_id().unwrap(), None);
        assert_eq!(page.get_pin_count().unwrap(), 0);
        assert_eq!(page.is_dirty().unwrap(), false);
        assert_eq!(page.get_data().unwrap(), [0; PAGE_SIZE]);
    }
}
