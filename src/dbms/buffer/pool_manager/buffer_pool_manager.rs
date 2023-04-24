use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::dbms::buffer::replacer::{BufferPoolReplacerError, IBufferPoolReplacer};
use crate::dbms::storage::disk::{DiskManagerError, IDiskManager};
use crate::dbms::storage::page::{IPage, Page, PageError, PAGE_SIZE};

#[derive(Debug)]
pub enum BufferPoolManagerError {
    /// Unable to free up a page when fetching a page from disk
    NoFrameAvailable,
    /// The requested page is not in the buffer pool
    PageNotInPool,
    /// A page is in use, e.g. when it's trying to be deleted
    PageInUse,
    ReplacerError(BufferPoolReplacerError),
    PageError(PageError),
    DiskManagerError(DiskManagerError),
}

impl From<BufferPoolReplacerError> for BufferPoolManagerError {
    fn from(e: BufferPoolReplacerError) -> Self {
        Self::ReplacerError(e)
    }
}

impl From<PageError> for BufferPoolManagerError {
    fn from(e: PageError) -> Self {
        Self::PageError(e)
    }
}

impl From<DiskManagerError> for BufferPoolManagerError {
    fn from(e: DiskManagerError) -> Self {
        Self::DiskManagerError(e)
    }
}

type ReadOnlyPage<'a> = RwLockReadGuard<'a, PageGeneric>;
type WritablePage<'a> = RwLockWriteGuard<'a, PageGeneric>;

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

type ReplacerGeneric = Box<dyn IBufferPoolReplacer + Send + Sync>;
type DiskManagerGeneric = Box<dyn IDiskManager + Send + Sync>;
type PageGeneric = Box<dyn IPage + Send + Sync>;

#[derive(Clone)]
struct BufferPoolManager {
    replacer: Arc<RwLock<ReplacerGeneric>>,
    disk_manager: Arc<RwLock<DiskManagerGeneric>>,
    /// page_id -> frame_id
    // Latch on the whole hashmap
    page_table: Arc<RwLock<HashMap<usize, usize>>>,
    // N.B. Latch on the whole array
    free_frames: Arc<RwLock<Vec<usize>>>,
    // N.B. Latch on each individual page, not the array itself
    pages: Arc<Vec<RwLock<PageGeneric>>>,
}

impl BufferPoolManager {
    #[allow(dead_code)]
    fn new(
        pool_size: usize,
        replacer: ReplacerGeneric,
        disk_manager: DiskManagerGeneric,
    ) -> BufferPoolManager {
        BufferPoolManager {
            replacer: Arc::new(RwLock::new(replacer)),
            disk_manager: Arc::new(RwLock::new(disk_manager)),
            page_table: Arc::new(RwLock::new(HashMap::new())),
            // All frames are free
            free_frames: Arc::new(RwLock::new((0..pool_size).collect())),
            // Fill frames with uninitialized pages with no page IDs
            pages: Arc::new(
                (0..pool_size)
                    .map(|_| RwLock::new(Box::new(Page::new(None)) as PageGeneric))
                    .collect(),
            ),
        }
    }

    fn get_freeable_frame_id(
        &self,
        replacer: &mut RwLockWriteGuard<ReplacerGeneric>,
    ) -> Result<usize, BufferPoolManagerError> {
        let mut free_frames = self.free_frames.write().unwrap();
        if let Some(f) = free_frames.pop() {
            return Ok(f);
        }
        if let Some(frame) = replacer.victim()? {
            Ok(frame)
        } else {
            Err(BufferPoolManagerError::NoFrameAvailable)
        }
    }

    /// Write a page to disk if it's dirty
    fn write_if_dirty(
        &self,
        page: &mut RwLockWriteGuard<PageGeneric>,
        disk_manager: &mut RwLockWriteGuard<DiskManagerGeneric>,
    ) -> Result<(), BufferPoolManagerError> {
        let page_dirty = page.is_dirty()?;
        let page_id = match page.get_page_id() {
            Ok(Some(id)) => id,
            Ok(None) => return Ok(()), // TODO: revisit
            Err(e) => return Err(BufferPoolManagerError::PageError(e)),
        };

        if page_dirty {
            let page_data = page.get_data()?;
            disk_manager.write_page(page_id, &page_data)?;
            page.set_clean()?;
        }

        Ok(())
    }

    /// Flush a frame to disk and prep it for a new page
    fn swap_frame(
        &self,
        frame_id: usize,
        new_page_id: usize,
        disk_manager: &mut RwLockWriteGuard<DiskManagerGeneric>,
        replacer: &mut RwLockWriteGuard<ReplacerGeneric>,
        page_table: &mut RwLockWriteGuard<HashMap<usize, usize>>,
        page: &mut RwLockWriteGuard<PageGeneric>,
    ) -> Result<(), BufferPoolManagerError> {
        if let Some(old_page_id) = page.get_page_id().unwrap() {
            self.write_if_dirty(page, disk_manager)?;

            page_table.remove(&old_page_id);
        }

        page_table.insert(new_page_id, frame_id);
        replacer.pin(frame_id)?;

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
            self.swap_frame(
                new_frame_id,
                page_id,
                &mut disk_manager,
                &mut replacer,
                &mut page_table,
                &mut page_to_overwrite,
            )?;

            let new_page_data = disk_manager.read_page(page_id)?;
            page_to_overwrite.overwrite(Some(page_id), new_page_data)?;
        }

        Ok(new_frame_id)
    }
}

impl IBufferPoolManager for BufferPoolManager {
    fn fetch_page(&self, page_id: usize) -> Result<ReadOnlyPage, BufferPoolManagerError> {
        let frame_id = self.fetch_page_frame(page_id)?;
        Ok(self.pages[frame_id].read().unwrap())
    }

    fn fetch_page_writable(&self, page_id: usize) -> Result<WritablePage, BufferPoolManagerError> {
        let frame_id = self.fetch_page_frame(page_id)?;
        Ok(self.pages[frame_id].write().unwrap())
    }

    fn new_page(&self) -> Result<Option<WritablePage>, BufferPoolManagerError> {
        let mut page_table = self.page_table.write().unwrap();
        let mut disk_manager = self.disk_manager.write().unwrap();
        let mut replacer = self.replacer.write().unwrap();

        // 1.   If all the pages in the buffer pool are pinned, return nullptr.
        {
            let pinless_page = self
                .pages
                .iter()
                .find(|page| page.read().unwrap().get_pin_count().unwrap() == 0);
            match pinless_page {
                Some(_) => {}
                None => {
                    return Ok(None);
                }
            };
        };

        // 0.   Make sure you call DiskManager::AllocatePage!
        let new_page_id = disk_manager.allocate_page()?;

        // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
        let frame_id = self.get_freeable_frame_id(&mut replacer).unwrap();
        let mut page_to_overwrite = self.pages[frame_id].write().unwrap();
        self.swap_frame(
            frame_id,
            new_page_id,
            &mut disk_manager,
            &mut replacer,
            &mut page_table,
            &mut page_to_overwrite,
        )?;

        // 3.   Update P's metadata, zero out memory and add P to the page table.
        page_to_overwrite.overwrite(Some(new_page_id), [0; PAGE_SIZE])?;
        page_to_overwrite.increase_pin_count()?;
        replacer.pin(frame_id)?;

        // 4.   Set the page ID output parameter. Return a pointer to P.
        Ok(Some(page_to_overwrite))
    }

    fn unpin_page(&self, page_id: usize, mark_dirty: bool) -> Result<(), BufferPoolManagerError> {
        let page_table = self.page_table.read().unwrap();
        let mut replacer = self.replacer.write().unwrap();

        if let Some(&frame_id) = page_table.get(&page_id) {
            let mut page = self.pages[frame_id].write().unwrap();

            page.decrease_pin_count()?;
            if mark_dirty {
                page.set_dirty()?;
            }

            if page.get_pin_count()? == 0 {
                replacer.unpin(frame_id)?;
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

            let data = page.get_data()?;
            disk_manager.write_page(page_id, &data)?;

            page.set_clean()?;

            Ok(())
        } else {
            Err(BufferPoolManagerError::PageNotInPool)
        }
    }

    fn delete_page(&self, page_id: usize) -> Result<(), BufferPoolManagerError> {
        let mut page_table = self.page_table.write().unwrap();
        let mut disk_manager = self.disk_manager.write().unwrap();
        let mut free_frames = self.free_frames.write().unwrap();

        // 1.   Search the page table for the requested page (P).
        if let Some(&frame_id) = page_table.get(&page_id) {
            let mut page = self.pages[frame_id].write().unwrap();

            // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
            match page.get_pin_count() {
                Ok(0) => {}
                Ok(_) => return Err(BufferPoolManagerError::PageInUse),
                Err(e) => return Err(BufferPoolManagerError::PageError(e)),
            };

            // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
            self.write_if_dirty(&mut page, &mut disk_manager)?;

            page_table.remove(&page_id);

            // 0.   Make sure you call DiskManager::DeallocatePage!
            disk_manager.deallocate_page(page_id)?;

            page.clear()?;

            free_frames.push(frame_id);
        }

        Ok(())
    }

    fn flush_all_pages(&self) -> Result<(), BufferPoolManagerError> {
        let mut disk_manager = self.disk_manager.write().unwrap();
        // Obtain the write latch on all pages
        let mut pages = self
            .pages
            .iter()
            .map(|page| page.write().unwrap())
            .collect::<Vec<_>>();

        for page in pages.iter_mut() {
            self.write_if_dirty(page, &mut disk_manager)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbms::buffer::replacer::clock_replacer::ClockReplacer;
    use crate::dbms::storage::disk::testing::InMemoryDiskManager;
    use rstest::*;

    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(10)]
    fn test_new_page(#[case] pool_size: usize) {
        let disk_manager = InMemoryDiskManager::new();
        let replacer = ClockReplacer::new(pool_size);
        let buffer_pool_manager =
            BufferPoolManager::new(pool_size, Box::new(replacer), Box::new(disk_manager));

        for _ in 0..pool_size {
            let page = buffer_pool_manager.new_page().unwrap();
            assert!(page.is_some());
        }

        let page = buffer_pool_manager.new_page().unwrap();
        assert!(page.is_none());
    }

    #[rstest]
    fn test_new_pages_threaded() {
        let pool_size = 100;
        let disk_manager = InMemoryDiskManager::new();
        let replacer = ClockReplacer::new(pool_size);
        let buffer_pool_manager =
            BufferPoolManager::new(pool_size, Box::new(replacer), Box::new(disk_manager));

        let mut threads = Vec::new();
        for _ in 0..10 {
            let buffer_pool_manager = buffer_pool_manager.clone();
            threads.push(std::thread::spawn(move || {
                for _ in 0..10 {
                    let page = buffer_pool_manager.new_page().unwrap();
                    assert!(page.is_some());
                }
            }));
        }

        for thread in threads {
            thread.join().unwrap();
        }

        for page_id in 0..100 {
            let page = buffer_pool_manager.fetch_page(page_id);
            assert!(page.is_ok());
        }
    }

    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(10)]
    fn test_delete_page(#[case] pool_size: usize) {
        let disk_manager = InMemoryDiskManager::new();
        let replacer = ClockReplacer::new(pool_size);
        let buffer_pool_manager =
            BufferPoolManager::new(pool_size, Box::new(replacer), Box::new(disk_manager));

        let mut page_ids = Vec::<usize>::new();

        for _ in 0..pool_size {
            let page = buffer_pool_manager.new_page().unwrap();
            assert!(page.is_some());
            page_ids.push(page.unwrap().get_page_id().unwrap().unwrap());
        }

        let page = buffer_pool_manager.new_page().unwrap();
        assert!(page.is_none());

        for page_id in page_ids {
            buffer_pool_manager.unpin_page(page_id, false).unwrap();
            buffer_pool_manager.delete_page(page_id).unwrap();
        }

        for _ in 0..pool_size {
            let page = buffer_pool_manager.new_page().unwrap();
            assert!(page.is_some());
        }
    }

    #[rstest]
    fn test_fetch_page() {
        let disk_manager = InMemoryDiskManager::new();
        let replacer = ClockReplacer::new(10);
        let buffer_pool_manager =
            BufferPoolManager::new(10, Box::new(replacer), Box::new(disk_manager));

        let page = buffer_pool_manager.fetch_page(0);
        assert!(page.is_err());

        let page = buffer_pool_manager.new_page().unwrap();
        assert!(page.is_some());
        let page_id = page.unwrap().get_page_id().unwrap().unwrap();
        buffer_pool_manager.unpin_page(page_id, false).unwrap();

        let page = buffer_pool_manager.fetch_page(page_id);
        assert!(page.is_ok());
    }
}
