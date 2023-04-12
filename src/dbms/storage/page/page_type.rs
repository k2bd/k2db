#[cfg(test)]
use mockall::automock;

use std::sync::{RwLock};

const PAGE_SIZE: usize = 4096;


#[cfg_attr(test, automock)]
pub trait IPage {
    fn get_data(&self) -> [u8; PAGE_SIZE];
    fn set_data(&mut self, data: [u8; PAGE_SIZE]);
    fn read_data(&self, offset: usize, size: usize) -> Vec<u8>;
    fn write_data(&mut self, offset: usize, data: &[u8]);
    fn get_page_id(&self) -> usize;
    fn is_dirty(&self) -> bool;
    fn increase_pin_count(&mut self);
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
        *page_data = data;
    }

    fn read_data(&self, offset: usize, size: usize) -> Vec<u8> {
        let data = self.data.read().unwrap();
        data[offset..offset + size].to_vec()
    }

    fn write_data(&mut self, offset: usize, data: &[u8]) {
        let mut page_data = self.data.write().unwrap();
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
}
