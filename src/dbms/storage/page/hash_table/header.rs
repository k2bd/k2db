use crate::dbms::{
    buffer::types::{ReadOnlyPage, WritablePage},
    storage::page::PageError,
    types::PageId,
};

#[derive(Debug, PartialEq)]
pub enum HashTableHeaderError {
    /// Provided page ID is not set
    NoPageId,
    PageError(PageError),
}

impl From<PageError> for HashTableHeaderError {
    fn from(e: PageError) -> Self {
        HashTableHeaderError::PageError(e)
    }
}

const PAGE_ENTRY_SIZE_BYTES: usize = (PageId::BITS / 8) as usize;
const PAGE_ID_OFFSET_BYTES: usize = 0;
const SIZE_OFFSET_BYTES: usize = PAGE_ENTRY_SIZE_BYTES;
const NEXT_IND_OFFSET_BYTES: usize = 2 * PAGE_ENTRY_SIZE_BYTES;
const LSN_OFFSET_BYTES: usize = 3 * PAGE_ENTRY_SIZE_BYTES;
const BLOCK_PAGE_IDS_START_OFFSET_BYTES: usize = 4 * PAGE_ENTRY_SIZE_BYTES;

/// Interact with a page as a hash table header page.
pub trait IHashTableHeaderPageRead {
    /// Get the page ID
    fn get_page_id(&self) -> Result<PageId, HashTableHeaderError>;
    /// Number of Key & Value pairs the hash table can hold
    fn get_size(&self) -> Result<u32, HashTableHeaderError>;
    /// The next index to add a new entry
    fn get_next_ind(&self) -> Result<u32, HashTableHeaderError>;
    /// The log sequence number
    fn get_lsn(&self) -> Result<u32, HashTableHeaderError>;
    /// Get the page ID at the given index
    fn get_block_page_id(&self, position: usize) -> Result<PageId, HashTableHeaderError>;
}

/// Interact with a page as a hash table header page.
pub trait IHashTableHeaderPageWrite: IHashTableHeaderPageRead {
    /// Set the page ID
    fn set_page_id(&mut self, page_id: PageId) -> Result<(), HashTableHeaderError>;
    /// Set the number of Key & Value pairs the hash table can hold
    fn set_size(&mut self, size: u32) -> Result<(), HashTableHeaderError>;
    /// Set the next index to add a new entry
    fn set_next_ind(&mut self, next_ind: u32) -> Result<(), HashTableHeaderError>;
    /// Set the log sequence number
    fn set_lsn(&mut self, lsn: u32) -> Result<(), HashTableHeaderError>;
    /// Set the page ID at the given index
    fn set_block_page_id(
        &mut self,
        position: usize,
        page_id: PageId,
    ) -> Result<(), HashTableHeaderError>;
}

pub struct ReadOnlyHashTableHeaderPage<'a> {
    page: ReadOnlyPage<'a>,
}

impl<'a> ReadOnlyHashTableHeaderPage<'a> {
    fn read_single_at_offset(&self, offset_bytes: usize) -> Result<u32, HashTableHeaderError> {
        let data = self.page.read_data(offset_bytes, PAGE_ENTRY_SIZE_BYTES)?;
        let result = u32::from_be_bytes(data.as_slice().try_into().unwrap());
        Ok(result)
    }
}

impl IHashTableHeaderPageRead for ReadOnlyHashTableHeaderPage<'_> {
    fn get_page_id(&self) -> Result<PageId, HashTableHeaderError> {
        self.read_single_at_offset(PAGE_ID_OFFSET_BYTES)
    }

    fn get_size(&self) -> Result<u32, HashTableHeaderError> {
        self.read_single_at_offset(SIZE_OFFSET_BYTES)
    }

    fn get_next_ind(&self) -> Result<u32, HashTableHeaderError> {
        self.read_single_at_offset(NEXT_IND_OFFSET_BYTES)
    }

    fn get_lsn(&self) -> Result<u32, HashTableHeaderError> {
        self.read_single_at_offset(LSN_OFFSET_BYTES)
    }

    fn get_block_page_id(&self, position: usize) -> Result<PageId, HashTableHeaderError> {
        self.read_single_at_offset(
            BLOCK_PAGE_IDS_START_OFFSET_BYTES + position * PAGE_ENTRY_SIZE_BYTES,
        )
    }
}

pub struct WritableHashTableHeaderPage<'a> {
    page: WritablePage<'a>,
}

impl<'a> WritableHashTableHeaderPage<'a> {
    fn read_single_at_offset(&self, offset_bytes: usize) -> Result<u32, HashTableHeaderError> {
        let data = self.page.read_data(offset_bytes, PAGE_ENTRY_SIZE_BYTES)?;
        let result = u32::from_be_bytes(data.as_slice().try_into().unwrap());
        Ok(result)
    }

    fn write_single_at_offset(
        &mut self,
        offset_bytes: usize,
        value: u32,
    ) -> Result<(), HashTableHeaderError> {
        self.page.write_data(offset_bytes, &value.to_be_bytes())?;
        Ok(())
    }

    #[allow(dead_code)]
    /// Initialize a header page to contain its page ID
    fn initialize(&mut self) -> Result<(), HashTableHeaderError> {
        let page_id = self.page.get_page_id()?;
        match page_id {
            Some(page_id) => {
                self.set_page_id(page_id)?;
                Ok(())
            }
            None => Err(HashTableHeaderError::NoPageId),
        }
    }
}

impl IHashTableHeaderPageRead for WritableHashTableHeaderPage<'_> {
    fn get_page_id(&self) -> Result<PageId, HashTableHeaderError> {
        self.read_single_at_offset(PAGE_ID_OFFSET_BYTES)
    }

    fn get_size(&self) -> Result<u32, HashTableHeaderError> {
        self.read_single_at_offset(SIZE_OFFSET_BYTES)
    }

    fn get_next_ind(&self) -> Result<u32, HashTableHeaderError> {
        self.read_single_at_offset(NEXT_IND_OFFSET_BYTES)
    }

    fn get_lsn(&self) -> Result<u32, HashTableHeaderError> {
        self.read_single_at_offset(LSN_OFFSET_BYTES)
    }

    fn get_block_page_id(&self, position: usize) -> Result<PageId, HashTableHeaderError> {
        self.read_single_at_offset(
            BLOCK_PAGE_IDS_START_OFFSET_BYTES + position * PAGE_ENTRY_SIZE_BYTES,
        )
    }
}

impl IHashTableHeaderPageWrite for WritableHashTableHeaderPage<'_> {
    fn set_page_id(&mut self, page_id: PageId) -> Result<(), HashTableHeaderError> {
        self.write_single_at_offset(PAGE_ID_OFFSET_BYTES, page_id)
    }

    fn set_size(&mut self, size: u32) -> Result<(), HashTableHeaderError> {
        self.write_single_at_offset(SIZE_OFFSET_BYTES, size)
    }

    fn set_next_ind(&mut self, next_ind: u32) -> Result<(), HashTableHeaderError> {
        self.write_single_at_offset(NEXT_IND_OFFSET_BYTES, next_ind)
    }

    fn set_lsn(&mut self, lsn: u32) -> Result<(), HashTableHeaderError> {
        self.write_single_at_offset(LSN_OFFSET_BYTES, lsn)
    }

    fn set_block_page_id(
        &mut self,
        position: usize,
        page_id: PageId,
    ) -> Result<(), HashTableHeaderError> {
        self.write_single_at_offset(
            BLOCK_PAGE_IDS_START_OFFSET_BYTES + position * PAGE_ENTRY_SIZE_BYTES,
            page_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::dbms::buffer::pool_manager::{
        testing::create_testing_pool_manager, IBufferPoolManager,
    };

    use super::*;
    use rstest::*;

    #[rstest]
    fn test_writable_page_read_page_id() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        let mut hash_table_header_page = WritableHashTableHeaderPage { page };

        hash_table_header_page.set_page_id(123).unwrap();

        let page_id = hash_table_header_page.get_page_id().unwrap();

        assert_eq!(page_id, 123);
    }

    #[rstest]
    fn test_threaded_set_read_page_id() {
        let pool_manager = create_testing_pool_manager(100);

        // Initialize a bunch of pages in threads
        let mut write_threads = Vec::new();
        {
            for _ in 0..11 {
                let buffer_pool_manager = pool_manager.clone();
                write_threads.push(std::thread::spawn(move || {
                    let page = buffer_pool_manager.new_page().unwrap();
                    let mut tmp_hash_table_header_page = WritableHashTableHeaderPage { page };
                    tmp_hash_table_header_page.initialize().unwrap();
                }));
            }
        }

        for thread in write_threads {
            thread.join().unwrap();
        }

        pool_manager.flush_all_pages().unwrap();

        // Show that we can read back the page ID from a page
        // (relying on the test logic that page IDs in the test pool manager count up from 0)
        let mut read_threads = Vec::new();
        {
            for i in 0..11 {
                let buffer_pool_manager = pool_manager.clone();
                read_threads.push(std::thread::spawn(move || {
                    let page = buffer_pool_manager.fetch_page(i).unwrap();
                    let hash_table_header_page_reader = ReadOnlyHashTableHeaderPage { page };

                    let page_id_header = hash_table_header_page_reader.get_page_id().unwrap();

                    assert_eq!(page_id_header, i);
                }));
            }
        }

        for thread in read_threads {
            thread.join().unwrap();
        }
    }

    #[rstest]
    fn test_threaded_read_size() {
        let pool_manager = create_testing_pool_manager(100);

        {
            for i in 0..11 {
                let page = pool_manager.new_page().unwrap();
                let mut tmp_hash_table_header_page = WritableHashTableHeaderPage { page };
                tmp_hash_table_header_page.initialize().unwrap();
                tmp_hash_table_header_page.set_size(i * 5).unwrap();
            }
        }

        pool_manager.flush_all_pages().unwrap();

        // Show that we can read back the page ID from a page
        // (relying on the test logic that page IDs in the test pool manager count up from 0)
        let mut read_threads = Vec::new();
        {
            for i in 0..11 {
                let buffer_pool_manager = pool_manager.clone();
                read_threads.push(std::thread::spawn(move || {
                    let page = buffer_pool_manager.fetch_page(i).unwrap();
                    let hash_table_header_page_reader = ReadOnlyHashTableHeaderPage { page };

                    let page_size = hash_table_header_page_reader.get_size().unwrap();

                    assert_eq!(page_size, i * 5);
                }));
            }
        }

        for thread in read_threads {
            thread.join().unwrap();
        }
    }

    #[rstest]
    fn test_threaded_read_next_ind() {
        let pool_manager = create_testing_pool_manager(100);

        {
            for i in 0..11 {
                let page = pool_manager.new_page().unwrap();
                let mut tmp_hash_table_header_page = WritableHashTableHeaderPage { page };
                tmp_hash_table_header_page.initialize().unwrap();
                tmp_hash_table_header_page.set_next_ind(i * 5).unwrap();
            }
        }

        pool_manager.flush_all_pages().unwrap();

        // Show that we can read back the page ID from a page
        // (relying on the test logic that page IDs in the test pool manager count up from 0)
        let mut read_threads = Vec::new();
        {
            for i in 0..11 {
                let buffer_pool_manager = pool_manager.clone();
                read_threads.push(std::thread::spawn(move || {
                    let page = buffer_pool_manager.fetch_page(i).unwrap();
                    let hash_table_header_page_reader = ReadOnlyHashTableHeaderPage { page };

                    let page_next_ind = hash_table_header_page_reader.get_next_ind().unwrap();

                    assert_eq!(page_next_ind, i * 5);
                }));
            }
        }

        for thread in read_threads {
            thread.join().unwrap();
        }
    }

    #[rstest]
    fn test_threaded_read_lsn() {
        let pool_manager = create_testing_pool_manager(100);

        {
            for i in 0..11 {
                let page = pool_manager.new_page().unwrap();
                let mut tmp_hash_table_header_page = WritableHashTableHeaderPage { page };
                tmp_hash_table_header_page.initialize().unwrap();
                tmp_hash_table_header_page.set_lsn(i * 5).unwrap();
            }
        }

        pool_manager.flush_all_pages().unwrap();

        // Show that we can read back the page ID from a page
        // (relying on the test logic that page IDs in the test pool manager count up from 0)
        let mut read_threads = Vec::new();
        {
            for i in 0..11 {
                let buffer_pool_manager = pool_manager.clone();
                read_threads.push(std::thread::spawn(move || {
                    let page = buffer_pool_manager.fetch_page(i).unwrap();
                    let hash_table_header_page_reader = ReadOnlyHashTableHeaderPage { page };

                    let page_lsn = hash_table_header_page_reader.get_lsn().unwrap();

                    assert_eq!(page_lsn, i * 5);
                }));
            }
        }

        for thread in read_threads {
            thread.join().unwrap();
        }
    }

    #[rstest]
    fn test_threaded_read_block_page_id() {
        let pool_manager = create_testing_pool_manager(100);

        {
            for i in 0..11 {
                let page = pool_manager.new_page().unwrap();
                let mut tmp_hash_table_header_page = WritableHashTableHeaderPage { page };
                tmp_hash_table_header_page.initialize().unwrap();

                tmp_hash_table_header_page
                    .set_block_page_id(10, i * 3)
                    .unwrap();
                tmp_hash_table_header_page
                    .set_block_page_id(11, i * 4)
                    .unwrap();
                tmp_hash_table_header_page
                    .set_block_page_id(12, i * 5)
                    .unwrap();
            }
        }

        pool_manager.flush_all_pages().unwrap();

        // Show that we can read back the page ID from a page
        // (relying on the test logic that page IDs in the test pool manager count up from 0)
        let mut read_threads = Vec::new();
        {
            for i in 0..11 {
                let buffer_pool_manager = pool_manager.clone();
                read_threads.push(std::thread::spawn(move || {
                    let page = buffer_pool_manager.fetch_page(i).unwrap();
                    let hash_table_header_page_reader = ReadOnlyHashTableHeaderPage { page };

                    assert_eq!(
                        hash_table_header_page_reader.get_block_page_id(10),
                        Ok(i * 3)
                    );
                    assert_eq!(
                        hash_table_header_page_reader.get_block_page_id(11),
                        Ok(i * 4)
                    );
                    assert_eq!(
                        hash_table_header_page_reader.get_block_page_id(12),
                        Ok(i * 5)
                    );
                }));
            }
        }

        for thread in read_threads {
            thread.join().unwrap();
        }
    }
}
