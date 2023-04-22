use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::dbms::buffer::replacer::{BufferPoolReplacerError, IBufferPoolReplacer};
use crate::dbms::storage::disk::{DiskManagerError, IDiskManager};
use crate::dbms::storage::page::{IPage, Page, PageData, PageError};

pub enum BufferPoolManagerError {
    /// Unable to free up a page when fetching a page from disk
    NoFrameAvailable,
    /// The requested page is not in the buffer pool
    PageNotInPool,
    ReplacerError(BufferPoolReplacerError),
    PageError(PageError),
    DiskManagerError(DiskManagerError),
}

type ReadOnlyPage<'a> = RwLockReadGuard<'a, Box<dyn IPage>>;
type WritablePage<'a> = RwLockWriteGuard<'a, Box<dyn IPage>>;

pub trait IBufferPoolManager {
    /// Fetch the requested page as readable from the buffer pool.
    fn fetch_page(&self, page_id: usize) -> Result<ReadOnlyPage, BufferPoolManagerError>;
    /// Fetch the requested page as writable from the buffer pool.
    fn fetch_page_writable(&self, page_id: usize) -> Result<WritablePage, BufferPoolManagerError>;
    /// Creates a new page in the buffer pool, returning it as writable.
    fn new_page(&self) -> Result<WritablePage, BufferPoolManagerError>;
    /// Unpin the target page from the buffer pool.
    fn unpin_page(&self, page_id: usize, mark_dirty: bool) -> Result<(), BufferPoolManagerError>;
    /// Flushes the target page to disk.
    fn flush_page(&self, page_id: usize) -> Result<(), BufferPoolManagerError>;
    /// Deletes a page from the buffer pool.
    fn delete_page(&self, page_id: usize) -> Result<(), BufferPoolManagerError>;
    /// Flushes all the pages in the buffer pool to disk.
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

    /// Fetch a page, from disk if needed, and return its frame ID
    fn fetch_page_frame(&self, page_id: usize) -> Result<usize, BufferPoolManagerError> {
        // 1.     Search the page table for the requested page (P).
        let mut page_table = self.page_table.write().unwrap();
        let mut replacer = self.replacer.write().unwrap();

        if let Some(&frame_id) = page_table.get(&page_id) {
            // 1.1    If P exists, pin it and return it immediately.
            {
                let mut page = self.pages[frame_id].write().unwrap();
                page.increase_pin_count().unwrap();
                replacer.pin(frame_id).unwrap();
            }
            return Ok(frame_id);
        }

        // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
        //        Note that pages are always found from the free list first.
        let mut free_frames = self.free_frames.write().unwrap();
        let new_frame_id = if let Some(f) = free_frames.pop() {
            f
        } else {
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

        Ok(new_frame_id)
    }
}

impl IBufferPoolManager for BufferPoolManager {
    fn fetch_page(&self, page_id: usize) -> Result<ReadOnlyPage, BufferPoolManagerError> {
        match self.fetch_page_frame(page_id) {
            Ok(frame_id) => Ok(self.pages[frame_id].read().unwrap()),
            Err(e) => Err(e),
        }
    }

    fn fetch_page_writable(&self, page_id: usize) -> Result<WritablePage, BufferPoolManagerError> {
        match self.fetch_page_frame(page_id) {
            Ok(frame_id) => Ok(self.pages[frame_id].write().unwrap()),
            Err(e) => Err(e),
        }
    }

    fn new_page(&self) -> Result<WritablePage, BufferPoolManagerError> {
        // 0.   Make sure you call DiskManager::AllocatePage!
        // 1.   If all the pages in the buffer pool are pinned, return nullptr.
        // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
        // 3.   Update P's metadata, zero out memory and add P to the page table.
        // 4.   Set the page ID output parameter. Return a pointer to P.

        todo!()
    }

    fn unpin_page(&self, page_id: usize, mark_dirty: bool) -> Result<(), BufferPoolManagerError> {
        let page_table = self.page_table.read().unwrap();
        let mut replacer = self.replacer.write().unwrap();

        if let Some(&frame_id) = page_table.get(&page_id) {
            let mut page = self.pages[frame_id].write().unwrap();

            page.decrease_pin_count().unwrap();
            if mark_dirty {
                page.set_dirty().unwrap();
            }

            if page.get_pin_count().unwrap() == 0 {
                replacer.unpin(frame_id).unwrap();
            }

            Ok(())
        } else {
            Err(BufferPoolManagerError::PageNotInPool)
        }
    }

    fn flush_page(&self, page_id: usize) -> Result<(), BufferPoolManagerError> {
        let page_table = self.page_table.read().unwrap();
        let mut disk_manager = self.disk_manager.write().unwrap();

        if let Some(&frame_id) = page_table.get(&page_id) {
            let mut page = self.pages[frame_id].write().unwrap();

            let page_data = page.get_data();
            match page_data {
                Ok(data) => {
                    let res = disk_manager.write_page(page_id, &data);
                    if let Err(e) = res {
                        return Err(BufferPoolManagerError::DiskManagerError(e));
                    }
                }
                Err(e) => return Err(BufferPoolManagerError::PageError(e)),
            }

            match page.set_clean() {
                Ok(_) => Ok(()),
                Err(e) => Err(BufferPoolManagerError::PageError(e)),
            }
        } else {
            Err(BufferPoolManagerError::PageNotInPool)
        }
    }

    fn delete_page(&self, page_id: usize) -> Result<(), BufferPoolManagerError> {
        todo!()
    }

    fn flush_all_pages(&self) -> Result<(), BufferPoolManagerError> {
        todo!()
    }
}
