use crate::dbms::{
    buffer::types::{ReadOnlyPage, WritablePage},
    storage::page::PageError,
    types::{PageId, NULL_PAGE_ID, PAGE_SIZE},
};

#[derive(Debug, PartialEq, Eq)]
pub enum HashTableHeaderExtensionError {
    PageError(PageError),
}

impl From<PageError> for HashTableHeaderExtensionError {
    fn from(e: PageError) -> Self {
        HashTableHeaderExtensionError::PageError(e)
    }
}

const PAGE_ENTRY_SIZE_BYTES: usize = (PageId::BITS / 8) as usize;
const HEADER_PAGE_ID_OFFSET_BYTES: usize = 0;
const PREVIOUS_EXTENSION_PAGE_OFFSET_BYTES: usize = PAGE_ENTRY_SIZE_BYTES;
const NEXT_EXTENSION_PAGE_OFFSET_BYTES: usize = 2 * PAGE_ENTRY_SIZE_BYTES;
const BLOCK_PAGE_IDS_START_OFFSET_BYTES: usize = 3 * PAGE_ENTRY_SIZE_BYTES;
const BLOCK_PAGE_IDS_COUNT: usize =
    (PAGE_SIZE - BLOCK_PAGE_IDS_START_OFFSET_BYTES) / PAGE_ENTRY_SIZE_BYTES;

/// Interact with a page as a hash table header page.
pub trait IHashTableHeaderExtensionPageRead {
    /// Get the page ID of the root header page
    fn get_header_page_id(&self) -> Result<PageId, HashTableHeaderExtensionError>;
    /// Get the page ID of the previous extension page, if there is one
    fn get_previous_extension_page_id(
        &self,
    ) -> Result<Option<PageId>, HashTableHeaderExtensionError>;
    /// Get the page ID of the next extension page, if there is one
    fn get_next_extension_page_id(&self) -> Result<Option<PageId>, HashTableHeaderExtensionError>;
    /// Get the page ID at the given index
    fn get_block_page_id(
        &self,
        position: usize,
    ) -> Result<Option<PageId>, HashTableHeaderExtensionError>;
}

/// Interact with a page as a hash table header page.
pub trait IHashTableHeaderExtensionPageWrite: IHashTableHeaderExtensionPageRead {
    /// Set the page ID of the root header page
    fn set_header_page_id(&mut self, page_id: PageId) -> Result<(), HashTableHeaderExtensionError>;
    /// Set the page ID of the previous extension page
    fn set_previous_extension_page_id(
        &mut self,
        extension_page_id: Option<PageId>,
    ) -> Result<(), HashTableHeaderExtensionError>;
    /// Set the page ID of the next extension page
    fn set_next_extension_page_id(
        &mut self,
        extension_page_id: Option<PageId>,
    ) -> Result<(), HashTableHeaderExtensionError>;
    /// Set the page ID at the given index
    fn set_block_page_id(
        &mut self,
        position: usize,
        page_id: Option<PageId>,
    ) -> Result<(), HashTableHeaderExtensionError>;
}

pub struct ReadOnlyHashTableHeaderExtensionPage<'a> {
    page: ReadOnlyPage<'a>,
}

impl<'a> ReadOnlyHashTableHeaderExtensionPage<'a> {
    fn read_single_at_offset(
        &self,
        offset_bytes: usize,
    ) -> Result<u32, HashTableHeaderExtensionError> {
        let data = self.page.read_data(offset_bytes, PAGE_ENTRY_SIZE_BYTES)?;
        let result = u32::from_be_bytes(data.as_slice().try_into().unwrap());
        Ok(result)
    }
}

impl IHashTableHeaderExtensionPageRead for ReadOnlyHashTableHeaderExtensionPage<'_> {
    fn get_header_page_id(&self) -> Result<PageId, HashTableHeaderExtensionError> {
        self.read_single_at_offset(HEADER_PAGE_ID_OFFSET_BYTES)
    }

    fn get_previous_extension_page_id(
        &self,
    ) -> Result<Option<PageId>, HashTableHeaderExtensionError> {
        match self.read_single_at_offset(PREVIOUS_EXTENSION_PAGE_OFFSET_BYTES)? {
            NULL_PAGE_ID => Ok(None),
            p => Ok(Some(p)),
        }
    }

    fn get_next_extension_page_id(&self) -> Result<Option<PageId>, HashTableHeaderExtensionError> {
        match self.read_single_at_offset(NEXT_EXTENSION_PAGE_OFFSET_BYTES)? {
            NULL_PAGE_ID => Ok(None),
            p => Ok(Some(p)),
        }
    }

    fn get_block_page_id(
        &self,
        position: usize,
    ) -> Result<Option<u32>, HashTableHeaderExtensionError> {
        match self.read_single_at_offset(
            BLOCK_PAGE_IDS_START_OFFSET_BYTES + position * PAGE_ENTRY_SIZE_BYTES,
        )? {
            NULL_PAGE_ID => Ok(None),
            page_id => Ok(Some(page_id)),
        }
    }
}

pub struct WritableHashTableHeaderExtensionPage<'a> {
    page: WritablePage<'a>,
}

impl<'a> WritableHashTableHeaderExtensionPage<'a> {
    fn read_single_at_offset(
        &self,
        offset_bytes: usize,
    ) -> Result<u32, HashTableHeaderExtensionError> {
        let data = self.page.read_data(offset_bytes, PAGE_ENTRY_SIZE_BYTES)?;
        let result = u32::from_be_bytes(data.as_slice().try_into().unwrap());
        Ok(result)
    }

    fn write_single_at_offset(
        &mut self,
        offset_bytes: usize,
        value: u32,
    ) -> Result<(), HashTableHeaderExtensionError> {
        self.page.write_data(offset_bytes, &value.to_be_bytes())?;
        Ok(())
    }

    /// Initialize a new header extension page
    fn initialize(
        &mut self,
        header_page_id: PageId,
        previous_extension_page_id: Option<PageId>,
        next_extension_page_id: Option<PageId>,
    ) -> Result<(), HashTableHeaderExtensionError> {
        self.set_header_page_id(header_page_id)?;
        self.set_previous_extension_page_id(previous_extension_page_id)?;
        self.set_next_extension_page_id(next_extension_page_id)?;
        for i in 0..BLOCK_PAGE_IDS_COUNT {
            self.set_block_page_id(i, None)?;
        }
        Ok(())
    }
}

impl IHashTableHeaderExtensionPageRead for WritableHashTableHeaderExtensionPage<'_> {
    fn get_header_page_id(&self) -> Result<PageId, HashTableHeaderExtensionError> {
        self.read_single_at_offset(HEADER_PAGE_ID_OFFSET_BYTES)
    }

    fn get_previous_extension_page_id(
        &self,
    ) -> Result<Option<PageId>, HashTableHeaderExtensionError> {
        match self.read_single_at_offset(PREVIOUS_EXTENSION_PAGE_OFFSET_BYTES)? {
            NULL_PAGE_ID => Ok(None),
            p => Ok(Some(p)),
        }
    }

    fn get_next_extension_page_id(&self) -> Result<Option<PageId>, HashTableHeaderExtensionError> {
        match self.read_single_at_offset(NEXT_EXTENSION_PAGE_OFFSET_BYTES)? {
            NULL_PAGE_ID => Ok(None),
            p => Ok(Some(p)),
        }
    }

    fn get_block_page_id(
        &self,
        position: usize,
    ) -> Result<Option<u32>, HashTableHeaderExtensionError> {
        match self.read_single_at_offset(
            BLOCK_PAGE_IDS_START_OFFSET_BYTES + position * PAGE_ENTRY_SIZE_BYTES,
        )? {
            NULL_PAGE_ID => Ok(None),
            page_id => Ok(Some(page_id)),
        }
    }
}

impl IHashTableHeaderExtensionPageWrite for WritableHashTableHeaderExtensionPage<'_> {
    fn set_header_page_id(&mut self, page_id: PageId) -> Result<(), HashTableHeaderExtensionError> {
        self.write_single_at_offset(HEADER_PAGE_ID_OFFSET_BYTES, page_id)
    }

    fn set_previous_extension_page_id(
        &mut self,
        extension_page_id: Option<PageId>,
    ) -> Result<(), HashTableHeaderExtensionError> {
        match extension_page_id {
            Some(page_id) => {
                self.write_single_at_offset(PREVIOUS_EXTENSION_PAGE_OFFSET_BYTES, page_id)
            }
            None => self.write_single_at_offset(PREVIOUS_EXTENSION_PAGE_OFFSET_BYTES, NULL_PAGE_ID),
        }
    }

    fn set_next_extension_page_id(
        &mut self,
        extension_page_id: Option<PageId>,
    ) -> Result<(), HashTableHeaderExtensionError> {
        match extension_page_id {
            Some(page_id) => self.write_single_at_offset(NEXT_EXTENSION_PAGE_OFFSET_BYTES, page_id),
            None => self.write_single_at_offset(NEXT_EXTENSION_PAGE_OFFSET_BYTES, NULL_PAGE_ID),
        }
    }

    fn set_block_page_id(
        &mut self,
        position: usize,
        page_id: Option<u32>,
    ) -> Result<(), HashTableHeaderExtensionError> {
        let pos = BLOCK_PAGE_IDS_START_OFFSET_BYTES + position * PAGE_ENTRY_SIZE_BYTES;
        match page_id {
            Some(p) => self.write_single_at_offset(pos, p),
            None => self.write_single_at_offset(pos, NULL_PAGE_ID),
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
    fn test_writable_extension_page_set_header_page_id() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        let mut ext_page = WritableHashTableHeaderExtensionPage { page };

        ext_page.set_header_page_id(123).unwrap();

        let page_id = ext_page.get_header_page_id().unwrap();
        assert_eq!(page_id, 123);
    }

    #[rstest]
    #[case(Some(123))]
    #[case(None)]
    fn test_writable_extension_page_set_previous_extension_page_id(
        #[case] ext_page_id: Option<PageId>,
    ) {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        let mut ext_page = WritableHashTableHeaderExtensionPage { page };

        ext_page.set_previous_extension_page_id(Some(999)).unwrap();

        ext_page
            .set_previous_extension_page_id(ext_page_id)
            .unwrap();

        let page_id = ext_page.get_previous_extension_page_id().unwrap();
        assert_eq!(page_id, ext_page_id);
    }

    #[rstest]
    #[case(Some(123))]
    #[case(None)]
    fn test_writable_extension_page_set_next_extension_page_id(
        #[case] ext_page_id: Option<PageId>,
    ) {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        let mut ext_page = WritableHashTableHeaderExtensionPage { page };

        ext_page.set_next_extension_page_id(Some(999)).unwrap();

        ext_page.set_next_extension_page_id(ext_page_id).unwrap();

        let page_id = ext_page.get_next_extension_page_id().unwrap();
        assert_eq!(page_id, ext_page_id);
    }

    #[rstest]
    #[case(Some(123))]
    #[case(None)]
    fn test_writable_extension_page_set_block_page_id(#[case] ext_page_id: Option<PageId>) {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        let mut ext_page = WritableHashTableHeaderExtensionPage { page };

        ext_page.set_block_page_id(100, Some(999)).unwrap();

        ext_page.set_block_page_id(100, ext_page_id).unwrap();

        let page_id = ext_page.get_block_page_id(100).unwrap();
        assert_eq!(page_id, ext_page_id);
    }

    #[rstest]
    #[case(Some(123), Some(456))]
    #[case(Some(123), None)]
    #[case(None, Some(456))]
    #[case(None, None)]
    fn test_writable_extension_page_initialize(
        #[case] prev_ext_page_id: Option<PageId>,
        #[case] next_ext_page_id: Option<PageId>,
    ) {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        let mut ext_page = WritableHashTableHeaderExtensionPage { page };

        ext_page
            .initialize(999, prev_ext_page_id, next_ext_page_id)
            .unwrap();

        assert_eq!(ext_page.get_header_page_id().unwrap(), 999);
        assert_eq!(
            ext_page.get_next_extension_page_id().unwrap(),
            next_ext_page_id
        );
        assert_eq!(
            ext_page.get_previous_extension_page_id().unwrap(),
            prev_ext_page_id
        );
        assert_eq!(ext_page.get_block_page_id(100).unwrap(), None);
    }

    #[rstest]
    fn test_threaded_extension_page() {
        let pool_manager = create_testing_pool_manager(100);

        {
            for i in 0..11 {
                {
                    let _p = pool_manager.new_page().unwrap();
                }
                pool_manager.unpin_page(i, false).unwrap();
            }
        }

        let mut write_threads = Vec::new();
        {
            for i in 0..11 {
                let bpm = pool_manager.clone();
                write_threads.push(std::thread::spawn(move || {
                    {
                        let page = bpm.fetch_page_writable(i).unwrap();
                        let mut writer = WritableHashTableHeaderExtensionPage { page };

                        writer.initialize(i, None, Some(i * 2)).unwrap();
                        writer.set_block_page_id(100, Some(i * 3)).unwrap();
                    }
                    bpm.unpin_page(i, true).unwrap();
                }));
            }
        }

        for thread in write_threads {
            thread.join().unwrap();
        }

        pool_manager.flush_all_pages().unwrap();

        let mut read_threads = Vec::new();
        {
            for i in 0..11 {
                let bpm = pool_manager.clone();
                read_threads.push(std::thread::spawn(move || {
                    {
                        let page = bpm.fetch_page(i).unwrap();
                        let reader = ReadOnlyHashTableHeaderExtensionPage { page };

                        assert_eq!(reader.get_header_page_id().unwrap(), i);
                    }
                    bpm.unpin_page(i, false).unwrap();
                }));
                let bpm = pool_manager.clone();
                read_threads.push(std::thread::spawn(move || {
                    {
                        let page = bpm.fetch_page(i).unwrap();
                        let reader = ReadOnlyHashTableHeaderExtensionPage { page };

                        assert_eq!(reader.get_previous_extension_page_id().unwrap(), None);
                    }
                    bpm.unpin_page(i, false).unwrap();
                }));
                let bpm = pool_manager.clone();
                read_threads.push(std::thread::spawn(move || {
                    {
                        let page = bpm.fetch_page(i).unwrap();
                        let reader = ReadOnlyHashTableHeaderExtensionPage { page };

                        assert_eq!(reader.get_next_extension_page_id().unwrap(), Some(i * 2));
                    }
                    bpm.unpin_page(i, false).unwrap();
                }));
                let bpm = pool_manager.clone();
                read_threads.push(std::thread::spawn(move || {
                    {
                        let page = bpm.fetch_page(i).unwrap();
                        let reader = ReadOnlyHashTableHeaderExtensionPage { page };

                        assert_eq!(reader.get_block_page_id(100).unwrap(), Some(i * 3));
                    }
                    bpm.unpin_page(i, false).unwrap();
                }));
            }
        }
    }
}
