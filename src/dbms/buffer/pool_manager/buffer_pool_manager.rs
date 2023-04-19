use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use crate::dbms::buffer::replacer::{BufferPoolReplacerError, IBufferPoolReplacer};
use crate::dbms::storage::disk::{DiskManagerError, IDiskManager};
use crate::dbms::storage::page::{IPage, Page, PageData, PageError};

pub enum BufferPoolManagerError {
    /// Unable to free up a page when fetching a page from disk
    NoFrameAvailable,
    ReplacerError(BufferPoolReplacerError),
    PageError(PageError),
    DiskManagerError(DiskManagerError),
}

pub trait IBufferPoolManager {
    fn fetch_page(
        &self,
        page_id: usize,
    ) -> Result<RwLockReadGuard<Box<dyn IPage>>, BufferPoolManagerError>;
    fn new_page(&self) -> Result<usize, BufferPoolManagerError>;
    fn unpin_page(&self, page_id: usize) -> Result<(), BufferPoolManagerError>;
    fn flush_page(&self, page_id: usize) -> Result<(), BufferPoolManagerError>;
    fn delete_page(&self, page_id: usize) -> Result<(), BufferPoolManagerError>;
    fn flush_all_pages(&self) -> Result<(), BufferPoolManagerError>;
}

struct BufferPoolManager {
    replacer: Arc<RwLock<Box<dyn IBufferPoolReplacer>>>,
    disk_manager: Arc<RwLock<Box<dyn IDiskManager>>>,
    /// page_id -> frame_id
    // Concurrent mutability on the hashmap
    page_table: Arc<RwLock<HashMap<usize, usize>>>,
    // N.B. Concurrent mutability on the array
    free_frames: Arc<RwLock<Vec<usize>>>,
    // N.B. Concurrent mutability on each individual page, not the array itself
    pages: Arc<Vec<RwLock<Box<dyn IPage>>>>,
}

impl BufferPoolManager {
    fn new(
        pool_size: usize,
        replacer: Arc<RwLock<Box<dyn IBufferPoolReplacer>>>,
        disk_manager: Arc<RwLock<Box<dyn IDiskManager>>>,
    ) -> BufferPoolManager {
        BufferPoolManager {
            replacer,
            disk_manager,
            page_table: Arc::new(RwLock::new(HashMap::new())),
            // All frames are free
            free_frames: Arc::new(RwLock::new((0..pool_size).collect())),
            // Fill frames with uninitialized pages with no page IDs
            pages: Arc::new(
                (0..pool_size)
                    .map(|_| RwLock::new(Box::new(Page::new(None)) as Box<dyn IPage>))
                    .collect(),
            ),
        }
    }
}

impl IBufferPoolManager for BufferPoolManager {
    fn fetch_page(
        &self,
        page_id: usize,
    ) -> Result<RwLockReadGuard<'_, Box<(dyn IPage)>>, BufferPoolManagerError> {
        // 1.     Search the page table for the requested page (P).
        let mut page_table = self.page_table.write().unwrap();
        let mut replacer = self.replacer.write().unwrap();
        if let Some(frame_id) = page_table.get(&page_id) {
            // 1.1    If P exists, pin it and return it immediately.
            {
                let mut page = self.pages[*frame_id].write().unwrap();
                page.increase_pin_count().unwrap();
                replacer.pin(*frame_id).unwrap();
            }
            let page = self.pages[*frame_id].read().unwrap();
            return Ok(page);
        }

        // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
        //        Note that pages are always found from the free list first.
        let mut free_frames = self.free_frames.write().unwrap();
        let new_frame_id = if let Some(f) = free_frames.pop() {
            f
        } else {
            let mut replacer = self.replacer.write().unwrap();
            let victim_res = replacer.victim();
            if let Ok(victim) = victim_res {
                if let Some(frame) = victim {
                    frame
                } else {
                    return Err(BufferPoolManagerError::NoFrameAvailable);
                }
            } else {
                return Err(BufferPoolManagerError::ReplacerError(
                    victim_res.unwrap_err(),
                ));
            }
        };

        {
            let mut disk_manager = self.disk_manager.write().unwrap();
            let mut page_to_overwrite = self.pages[new_frame_id].write().unwrap();

            if let Some(old_page_id) = page_to_overwrite.get_page_id().unwrap() {
                // 2.     If R is dirty, write it back to the disk.
                if page_to_overwrite.is_dirty().unwrap() {
                    let page_data = page_to_overwrite.get_data().unwrap();

                    let res = disk_manager.write_page(old_page_id, &page_data);
                    if let Err(e) = res {
                        return Err(BufferPoolManagerError::DiskManagerError(e));
                    }
                }

                // 3.     Delete R from the page table and insert P.
                page_table.remove(&old_page_id);
                page_table.insert(page_id, new_frame_id);
            }

            // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
            let new_frame_pin_res = replacer.pin(new_frame_id);
            if let Err(e) = new_frame_pin_res {
                return Err(BufferPoolManagerError::ReplacerError(e));
            }

            let new_data_res = disk_manager.read_page(page_id);
            match new_data_res {
                Ok(new_page_data) => {
                    page_to_overwrite
                        .overwrite(Some(page_id), new_page_data)
                        .unwrap();
                }
                Err(e) => return Err(BufferPoolManagerError::DiskManagerError(e)),
            }
        }

        // Return a readable copy of the new page
        let new_page = self.pages[new_frame_id].read().unwrap();
        Ok(new_page)
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
