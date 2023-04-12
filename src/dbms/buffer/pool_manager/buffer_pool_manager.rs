#[cfg(test)]
use mockall::automock;

use std::collections::HashMap;
use std::sync::RwLock;

use crate::dbms::buffer::replacer::{IBufferPoolReplacer, BufferPoolReplacerError};
use crate::dbms::storage::disk::IDiskManager;

pub enum BufferPoolManagerError {
    ReplacerError(BufferPoolReplacerError),
}

#[cfg_attr(test, automock)]
pub trait IBufferPoolManager {
    fn fetch_page(&self, page_id: usize) -> Result<Vec<u8>, BufferPoolManagerError>;
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
}

impl BufferPoolManager {
    fn new(
        pool_size: usize,
        replacer: Box<dyn IBufferPoolReplacer>,
        disk_manager: Box<dyn IDiskManager>,
    ) -> BufferPoolManager {
        let mut free_frames = Vec::new();
        for i in 0..pool_size {
            free_frames.push(i);
        }

        BufferPoolManager {
            replacer: RwLock::new(replacer),
            disk_manager: RwLock::new(disk_manager),
            page_table: RwLock::new(HashMap::new()),
            free_frames: RwLock::new(free_frames),
        }
    }
}

impl IBufferPoolManager for BufferPoolManager {
    fn fetch_page(&self, page_id: usize) -> Result<Vec<u8>, BufferPoolManagerError> {
        {
            let page_table = self.page_table.read().unwrap();

            if let Some(frame_id) = page_table.get(&page_id) {
                // Page ID is already in buffer pool, pin it and return the frame

                let mut replacer = self.replacer.write().unwrap();
                let pin_res = replacer.pin(*frame_id);
                if pin_res.is_err() {
                    return Err(BufferPoolManagerError::ReplacerError(
                        pin_res.err().unwrap(),
                    ));
                }

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
