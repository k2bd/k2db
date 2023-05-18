#[cfg(test)]
use crate::dbms::{storage::disk::testing::InMemoryDiskManager, buffer::replacer::clock_replacer::ClockReplacer};

#[cfg(test)]
use super::BufferPoolManager;

#[cfg(test)]
pub fn create_testing_pool_manager(pool_size: usize) -> BufferPoolManager {
    let disk_manager = InMemoryDiskManager::new();
    let replacer = ClockReplacer::new(pool_size);
    BufferPoolManager::new(pool_size, Box::new(replacer), Box::new(disk_manager))
}