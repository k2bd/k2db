use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::dbms::buffer::replacer::{BufferPoolReplacerError, IBufferPoolReplacer};
use crate::dbms::storage::disk::{DiskManagerError, IDiskManager};
use crate::dbms::storage::page::{IPage, Page, PageData, PageError};

#[derive(Debug)]
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
    fn new_page(&self) -> Result<Option<WritablePage>, BufferPoolManagerError>;
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

    fn get_freeable_frame_id(
        &self,
        replacer: &mut RwLockWriteGuard<Box<dyn IBufferPoolReplacer>>,
    ) -> Result<usize, BufferPoolManagerError> {
        let mut free_frames = self.free_frames.write().unwrap();
        if let Some(f) = free_frames.pop() {
            Ok(f)
        } else {
            let victim_res = replacer.victim();
            if let Ok(victim) = victim_res {
                if let Some(frame) = victim {
                    Ok(frame)
                } else {
                    Err(BufferPoolManagerError::NoFrameAvailable)
                }
            } else {
                Err(BufferPoolManagerError::ReplacerError(
                    victim_res.unwrap_err(),
                ))
            }
        }
    }

    /// Flush a page to disk and prep it for a new page
    fn swap_frame(
        &self,
        frame_id: usize,
        page_id: usize,
        disk_manager: &mut RwLockWriteGuard<Box<dyn IDiskManager>>,
        replacer: &mut RwLockWriteGuard<Box<dyn IBufferPoolReplacer>>,
        page_table: &mut RwLockWriteGuard<HashMap<usize, usize>>,
        page: &mut RwLockWriteGuard<Box<dyn IPage>>,
    ) -> Result<(), BufferPoolManagerError> {
        if let Some(old_page_id) = page.get_page_id().unwrap() {
            if page.is_dirty().unwrap() {
                let page_data = page.get_data().unwrap();

                let res = disk_manager.write_page(old_page_id, &page_data);
                if let Err(e) = res {
                    return Err(BufferPoolManagerError::DiskManagerError(e));
                }
            }

            page_table.remove(&old_page_id);
            page_table.insert(page_id, frame_id);
        }

        let new_frame_pin_res = replacer.pin(frame_id);
        if let Err(e) = new_frame_pin_res {
            return Err(BufferPoolManagerError::ReplacerError(e));
        }

        Ok(())
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
        let new_frame_id = self.get_freeable_frame_id(&mut replacer).unwrap();

        {
            let mut disk_manager = self.disk_manager.write().unwrap();
            let mut page_to_overwrite = self.pages[new_frame_id].write().unwrap();

            // 2.     If R is dirty, write it back to the disk.
            // 3.     Delete R from the page table and insert P.
            // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
            match self.swap_frame(
                new_frame_id,
                page_id,
                &mut disk_manager,
                &mut replacer,
                &mut page_table,
                &mut page_to_overwrite,
            ) {
                Ok(_) => {}
                Err(e) => return Err(e),
            };

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

    fn find_matching_page(
        &self,
        predicate: impl Fn(&dyn IPage) -> bool,
    ) -> Result<Option<usize>, BufferPoolManagerError> {
        for (page_id, frame_id) in self.page_table.read().unwrap().iter() {
            let page = self.pages[*frame_id].read().unwrap();
            if predicate(page.as_ref()) {
                return Ok(Some(*page_id));
            }
        }
        Ok(None)
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

    fn new_page(&self) -> Result<Option<WritablePage>, BufferPoolManagerError> {
        let mut page_table = self.page_table.write().unwrap();
        let mut disk_manager = self.disk_manager.write().unwrap();
        let mut replacer = self.replacer.write().unwrap();

        // 1.   If all the pages in the buffer pool are pinned, return nullptr.
        match self.find_matching_page(|page| page.get_pin_count() == Ok(0)) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return Ok(None);
            }
            Err(e) => {
                return Err(e);
            }
        };

        // 0.   Make sure you call DiskManager::AllocatePage!
        let new_page_id = match disk_manager.allocate_page() {
            Ok(page_id) => page_id,
            Err(e) => return Err(BufferPoolManagerError::DiskManagerError(e)),
        };

        // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
        let frame_id = self.get_freeable_frame_id(&mut replacer).unwrap();
        let mut page_to_overwrite = self.pages[frame_id].write().unwrap();
        match self.swap_frame(
            frame_id,
            new_page_id,
            &mut disk_manager,
            &mut replacer,
            &mut page_table,
            &mut page_to_overwrite,
        ) {
            Ok(_) => {}
            Err(e) => return Err(e),
        };

        // 3.   Update P's metadata, zero out memory and add P to the page table.
        match page_to_overwrite.clear() {
            Ok(_) => {}
            Err(e) => return Err(BufferPoolManagerError::PageError(e)),
        }
        match page_to_overwrite.increase_pin_count() {
            Ok(_) => {}
            Err(e) => return Err(BufferPoolManagerError::PageError(e)),
        }
        match replacer.pin(frame_id) {
            Ok(_) => {}
            Err(e) => return Err(BufferPoolManagerError::ReplacerError(e)),
        }

        // 4.   Set the page ID output parameter. Return a pointer to P.
        Ok(Some(page_to_overwrite))
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
