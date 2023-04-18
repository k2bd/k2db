#[cfg(test)]
use mockall::automock;

use std::collections::HashMap;
use std::sync::{RwLock, RwLockReadGuard};

use crate::dbms::buffer::replacer::{BufferPoolReplacerError, IBufferPoolReplacer};
use crate::dbms::storage::disk::IDiskManager;
use crate::dbms::storage::page::{IPage, Page, PAGE_SIZE, PageData};

pub enum BufferPoolManagerError {
    ReplacerError(BufferPoolReplacerError),
}

pub trait IBufferPoolManager<'a> {
    fn fetch_page(&self, page_id: usize) -> Result<PageData, BufferPoolManagerError>;
    fn new_page(&self) -> Result<usize, BufferPoolManagerError>;
    fn unpin_page(&self, page_id: usize) -> Result<(), BufferPoolManagerError>;
    fn flush_page(&self, page_id: usize) -> Result<(), BufferPoolManagerError>;
    fn delete_page(&self, page_id: usize) -> Result<(), BufferPoolManagerError>;
    fn flush_all_pages(&self) -> Result<(), BufferPoolManagerError>;
}

struct BufferPoolManager {
    replacer: RwLock<Box<dyn IBufferPoolReplacer>>,
    disk_manager: RwLock<Box<dyn IDiskManager>>,
    /// page_id -> frame_id
    page_table: RwLock<HashMap<usize, usize>>,
    free_frames: RwLock<Vec<usize>>,
    pages: Vec<Box<dyn IPage>>,
}

impl BufferPoolManager {
    pub fn new(
        pool_size: usize,
        replacer: Box<dyn IBufferPoolReplacer>,
        disk_manager: Box<dyn IDiskManager>,
    ) -> BufferPoolManager {
        BufferPoolManager {
            replacer: RwLock::new(replacer),
            disk_manager: RwLock::new(disk_manager),
            page_table: RwLock::new(HashMap::new()),
            free_frames: RwLock::new((0..pool_size).collect::<Vec<_>>()),
            pages: (0..pool_size)
                .map(|i| Box::new(Page::new(i)) as _)
                .collect(),
        }
    }
}

impl<'a> IBufferPoolManager<'a> for BufferPoolManager {
    fn fetch_page(&self, page_id: usize) -> Result<RwLockReadGuard<'a, [u8; PAGE_SIZE]>, BufferPoolManagerError> {
        {
            let page_table = self.page_table.read().unwrap();

            if let Some(frame_id) = page_table.get(&page_id) {
                // Page ID is already in buffer pool, pin it and return the frame
                let mut replacer = self.replacer.write().unwrap();
                let page = &self.pages[*frame_id];

                let pin_res = replacer.pin(*frame_id);
                if pin_res.is_err() {
                    return Err(BufferPoolManagerError::ReplacerError(
                        pin_res.err().unwrap(),
                    ));
                }

                return Ok(page.get_data());
            }
        }

        todo!()
    }

    fn new_page(&self) -> Result<usize, BufferPoolManagerError> {
        todo!()
    }

    fn unpin_page(&self, page_id: usize) -> Result<(), BufferPoolManagerError> {
        todo!()
    }

    fn flush_page(&self, page_id: usize) -> Result<(), BufferPoolManagerError> {
        todo!()
    }

    fn delete_page(&self, page_id: usize) -> Result<(), BufferPoolManagerError> {
        todo!()
    }

    fn flush_all_pages(&self) -> Result<(), BufferPoolManagerError> {
        todo!()
    }
}
