use std::slice::Iter;

use crate::dbms::{
    buffer::types::{ReadOnlyPage, WritablePage},
    storage::page::PageError,
    types::{PageId, NULL_PAGE_ID, PAGE_SIZE},
};

#[derive(Debug, PartialEq, Eq)]
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
const EXTENSION_PAGE_OFFSET_BYTES: usize = 4 * PAGE_ENTRY_SIZE_BYTES;
const BLOCK_PAGE_IDS_START_OFFSET_BYTES: usize = 5 * PAGE_ENTRY_SIZE_BYTES;
const BLOCK_PAGE_IDS_COUNT: usize =
    (PAGE_SIZE - BLOCK_PAGE_IDS_START_OFFSET_BYTES) / PAGE_ENTRY_SIZE_BYTES;

/// Interact with a page as a hash table header page.
pub trait IHashTableHeaderPageRead<'a> {
    /// Get the page ID
    fn get_page_id(&self) -> Result<PageId, HashTableHeaderError>;
    /// Number of Key & Value pairs the hash table can hold
    fn get_size(&self) -> Result<u32, HashTableHeaderError>;
    /// The next index to add a new entry
    fn get_next_ind(&self) -> Result<u32, HashTableHeaderError>;
    /// The log sequence number
    fn get_lsn(&self) -> Result<u32, HashTableHeaderError>;
    /// Get the page ID at the given index
    fn get_block_page_id(&self, position: usize) -> Result<Option<PageId>, HashTableHeaderError>;
    /// Get the page ID of the first header extension page, if there is one
    fn get_extension_page_id(&self) -> Result<Option<PageId>, HashTableHeaderError>;
    /// Iterate over block page IDs within the header page (excluding extensions)
    fn iter_block_page_ids<'b>(&'b self) -> BlockPageIdIterator<'b, 'a>;
}

/// Interact with a page as a hash table header page.
pub trait IHashTableHeaderPageWrite<'a>: IHashTableHeaderPageRead<'a> {
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
        page_id: Option<PageId>,
    ) -> Result<(), HashTableHeaderError>;
    /// Set the ID of the first header extension page, or set it to None
    fn set_extension_page_id(
        &mut self,
        extension_page_id: Option<PageId>,
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

impl<'a> IHashTableHeaderPageRead<'a> for ReadOnlyHashTableHeaderPage<'a> {
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

    fn get_block_page_id(&self, position: usize) -> Result<Option<PageId>, HashTableHeaderError> {
        let res = self.read_single_at_offset(
            BLOCK_PAGE_IDS_START_OFFSET_BYTES + position * PAGE_ENTRY_SIZE_BYTES,
        )?;

        match res {
            NULL_PAGE_ID => Ok(None),
            page_id => Ok(Some(page_id)),
        }
    }

    fn get_extension_page_id(&self) -> Result<Option<PageId>, HashTableHeaderError> {
        let res = self.read_single_at_offset(EXTENSION_PAGE_OFFSET_BYTES)?;

        match res {
            NULL_PAGE_ID => Ok(None),
            page_id => Ok(Some(page_id)),
        }
    }

    fn iter_block_page_ids<'b>(&'b self) -> BlockPageIdIterator<'b, 'a> {
        BlockPageIdIterator {
            header_page: self,
            current_position: 0,
            max_position: BLOCK_PAGE_IDS_COUNT,
            _lifetime: std::marker::PhantomData,
        }
    }
}

pub struct BlockPageIdIterator<'a, 'b> {
    header_page: &'a dyn IHashTableHeaderPageRead<'a>,
    current_position: usize,
    max_position: usize,
    _lifetime: std::marker::PhantomData<&'b ()>,
}

impl<'a, 'b> Iterator for BlockPageIdIterator<'a, 'b> {
    type Item = PageId;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_position < self.max_position {
            let page_id = self
                .header_page
                .get_block_page_id(self.current_position)
                .unwrap();
            self.current_position += 1;
            if let Some(page_id) = page_id {
                return Some(page_id);
            }
        }
        None
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

    pub fn new(page: WritablePage<'a>) -> Self {
        Self { page }
    }

    /// Initialize a header page to contain its page ID
    pub fn initialize(&mut self, size: u32) -> Result<(), HashTableHeaderError> {
        let page_id = self.page.get_page_id()?;
        match page_id {
            Some(page_id) => {
                self.set_page_id(page_id)?;
                self.set_extension_page_id(None)?;
                for i in 0..BLOCK_PAGE_IDS_COUNT {
                    self.set_block_page_id(i, None)?;
                }
                self.set_size(size);
                Ok(())
            }
            None => Err(HashTableHeaderError::NoPageId),
        }
    }
}

impl<'a> IHashTableHeaderPageRead<'a> for WritableHashTableHeaderPage<'a> {
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

    fn get_block_page_id(&self, position: usize) -> Result<Option<PageId>, HashTableHeaderError> {
        let res = self.read_single_at_offset(
            BLOCK_PAGE_IDS_START_OFFSET_BYTES + position * PAGE_ENTRY_SIZE_BYTES,
        )?;

        match res {
            NULL_PAGE_ID => Ok(None),
            page_id => Ok(Some(page_id)),
        }
    }

    fn get_extension_page_id(&self) -> Result<Option<PageId>, HashTableHeaderError> {
        let res = self.read_single_at_offset(EXTENSION_PAGE_OFFSET_BYTES)?;

        match res {
            NULL_PAGE_ID => Ok(None),
            page_id => Ok(Some(page_id)),
        }
    }

    fn iter_block_page_ids<'b>(&'b self) -> BlockPageIdIterator<'b, 'a> {
        BlockPageIdIterator {
            header_page: self,
            current_position: 0,
            max_position: BLOCK_PAGE_IDS_COUNT,
            _lifetime: std::marker::PhantomData,
        }
    }
}

impl<'a> IHashTableHeaderPageWrite<'a> for WritableHashTableHeaderPage<'a> {
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
        page_id: Option<PageId>,
    ) -> Result<(), HashTableHeaderError> {
        let pos = BLOCK_PAGE_IDS_START_OFFSET_BYTES + position * PAGE_ENTRY_SIZE_BYTES;
        match page_id {
            Some(p) => self.write_single_at_offset(pos, p),
            None => self.write_single_at_offset(pos, NULL_PAGE_ID),
        }
    }

    fn set_extension_page_id(
        &mut self,
        extension_page_id: Option<PageId>,
    ) -> Result<(), HashTableHeaderError> {
        match extension_page_id {
            Some(page_id) => self.write_single_at_offset(EXTENSION_PAGE_OFFSET_BYTES, page_id),
            None => self.write_single_at_offset(EXTENSION_PAGE_OFFSET_BYTES, NULL_PAGE_ID),
        }
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
    fn test_writable_page_read_size() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        let mut hash_table_header_page = WritableHashTableHeaderPage { page };

        hash_table_header_page.set_size(123).unwrap();

        let page_size = hash_table_header_page.get_size().unwrap();

        assert_eq!(page_size, 123);
    }

    #[rstest]
    fn test_writable_page_read_next_ind() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        let mut hash_table_header_page = WritableHashTableHeaderPage { page };

        hash_table_header_page.set_next_ind(123).unwrap();

        let page_next_ind = hash_table_header_page.get_next_ind().unwrap();

        assert_eq!(page_next_ind, 123);
    }

    #[rstest]
    fn test_writable_page_read_lsn() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        let mut hash_table_header_page = WritableHashTableHeaderPage { page };

        hash_table_header_page.set_lsn(123).unwrap();

        let page_lsn = hash_table_header_page.get_lsn().unwrap();

        assert_eq!(page_lsn, 123);
    }

    #[rstest]
    fn test_writable_page_read_block_page_id() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        let mut hash_table_header_page = WritableHashTableHeaderPage { page };

        hash_table_header_page
            .set_block_page_id(10, Some(123))
            .unwrap();

        let page_block_page_id = hash_table_header_page.get_block_page_id(10).unwrap();

        assert_eq!(page_block_page_id, Some(123));
    }

    #[rstest]
    fn test_writable_page_set_block_page_id_to_none() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        let mut hash_table_header_page = WritableHashTableHeaderPage { page };

        hash_table_header_page
            .set_block_page_id(10, Some(123))
            .unwrap();
        let page_block_page_id = hash_table_header_page.get_block_page_id(10).unwrap();
        assert_eq!(page_block_page_id, Some(123));

        hash_table_header_page.set_block_page_id(10, None).unwrap();
        let page_block_page_id = hash_table_header_page.get_block_page_id(10).unwrap();
        assert_eq!(page_block_page_id, None);
    }

    #[rstest]
    fn test_writable_page_read_header_extension_page_id() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        let mut hash_table_header_page = WritableHashTableHeaderPage { page };

        hash_table_header_page
            .set_extension_page_id(Some(123))
            .unwrap();
        let page_header_extension_page_id = hash_table_header_page.get_extension_page_id().unwrap();

        assert_eq!(page_header_extension_page_id, Some(123));

        hash_table_header_page.set_extension_page_id(None).unwrap();
        let page_header_extension_page_id_2 =
            hash_table_header_page.get_extension_page_id().unwrap();

        assert_eq!(page_header_extension_page_id_2, None);
    }

    #[rstest]
    fn test_writable_page_iter_block_page_ids() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();
        let mut hash_table_header_page = WritableHashTableHeaderPage { page };

        hash_table_header_page.initialize(10).unwrap();

        hash_table_header_page
            .set_block_page_id(100, Some(123))
            .unwrap();
        hash_table_header_page
            .set_block_page_id(101, Some(234))
            .unwrap();
        hash_table_header_page
            .set_block_page_id(150, Some(345))
            .unwrap();

        let mut iter = hash_table_header_page.iter_block_page_ids();
        let mut block_page_ids = Vec::new();
        while let Some(block_page_id) = iter.next() {
            block_page_ids.push(block_page_id);
        }

        assert_eq!(block_page_ids, vec![123, 234, 345]);
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
                    tmp_hash_table_header_page.initialize(10).unwrap();
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
                tmp_hash_table_header_page.initialize(10).unwrap();
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
                tmp_hash_table_header_page.initialize(10).unwrap();
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
                tmp_hash_table_header_page.initialize(10).unwrap();
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
                tmp_hash_table_header_page.initialize(10).unwrap();

                tmp_hash_table_header_page
                    .set_block_page_id(10, Some(i * 3))
                    .unwrap();
                tmp_hash_table_header_page
                    .set_block_page_id(11, Some(i * 4))
                    .unwrap();
                tmp_hash_table_header_page
                    .set_block_page_id(12, Some(1))
                    .unwrap();
                tmp_hash_table_header_page
                    .set_block_page_id(12, None)
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
                        Ok(Some(i * 3))
                    );
                    assert_eq!(
                        hash_table_header_page_reader.get_block_page_id(11),
                        Ok(Some(i * 4))
                    );
                    assert_eq!(
                        hash_table_header_page_reader.get_block_page_id(12),
                        Ok(None)
                    );
                }));
            }
        }

        for thread in read_threads {
            thread.join().unwrap();
        }
    }

    #[rstest]
    #[case(Some(5))]
    #[case(None)]
    fn test_threaded_get_extension_page_id(#[case] ext_page_factor: Option<usize>) {
        let pool_manager = create_testing_pool_manager(100);

        {
            for i in 0..11 {
                let ext_page_id = match ext_page_factor {
                    Some(f) => Some(i * 5),
                    None => None,
                };

                let page = pool_manager.new_page().unwrap();
                let mut tmp_hash_table_header_page = WritableHashTableHeaderPage { page };
                tmp_hash_table_header_page.initialize(10).unwrap();
                tmp_hash_table_header_page
                    .set_extension_page_id(ext_page_id)
                    .unwrap();
            }
        }

        pool_manager.flush_all_pages().unwrap();

        // Show that we can read back the page ID from a page
        // (relying on the test logic that page IDs in the test pool manager count up from 0)
        let mut read_threads = Vec::new();
        {
            for i in 0..11 {
                let ext_page_id = match ext_page_factor {
                    Some(f) => Some(i * 5),
                    None => None,
                };

                let buffer_pool_manager = pool_manager.clone();
                read_threads.push(std::thread::spawn(move || {
                    let page = buffer_pool_manager.fetch_page(i).unwrap();
                    let hash_table_header_page_reader = ReadOnlyHashTableHeaderPage { page };

                    let page_lsn = hash_table_header_page_reader
                        .get_extension_page_id()
                        .unwrap();

                    assert_eq!(page_lsn, ext_page_id);
                }));
            }
        }

        for thread in read_threads {
            thread.join().unwrap();
        }
    }

    #[rstest]
    fn test_threaded_iter_block_page_ids() {
        let pool_manager = create_testing_pool_manager(100);

        {
            for i in 0..11 {
                let page = pool_manager.new_page().unwrap();
                let mut tmp_hash_table_header_page = WritableHashTableHeaderPage { page };
                tmp_hash_table_header_page.initialize(10).unwrap();
                tmp_hash_table_header_page
                    .set_block_page_id(10, Some(i * 3))
                    .unwrap();
                tmp_hash_table_header_page
                    .set_block_page_id(11, Some(i * 4))
                    .unwrap();
                tmp_hash_table_header_page
                    .set_block_page_id(12, Some(1))
                    .unwrap();
                tmp_hash_table_header_page
                    .set_block_page_id(12, None)
                    .unwrap();
                tmp_hash_table_header_page
                    .set_block_page_id(100, Some(123))
                    .unwrap();
            }
        }

        pool_manager.flush_all_pages().unwrap();

        let mut read_threads = Vec::new();
        {
            for i in 0..11 {
                for _ in 0..5 {
                    let buffer_pool_manager = pool_manager.clone();
                    read_threads.push(std::thread::spawn(move || {
                        let page = buffer_pool_manager.fetch_page(i).unwrap();
                        let hash_table_header_page_reader = ReadOnlyHashTableHeaderPage { page };

                        let mut iter = hash_table_header_page_reader.iter_block_page_ids();
                        let mut block_page_ids = Vec::new();
                        while let Some(block_page_id) = iter.next() {
                            block_page_ids.push(block_page_id);
                        }

                        assert_eq!(block_page_ids, vec![i * 3, i * 4, 123]);
                    }));
                }
            }
        }

        for thread in read_threads {
            thread.join().unwrap();
        }
    }
}
