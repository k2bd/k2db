use crate::dbms::{
    buffer::types::{ReadOnlyPage, WritablePage},
    storage::page::PageError,
    types::{PageId, NULL_PAGE_ID, PAGE_SIZE},
};

#[derive(Debug, PartialEq, Eq)]
pub enum HashTableHeaderExtensionError {
    /// No more space for block page IDs
    NoMoreCapacity,
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
pub trait IHashTableHeaderExtensionPageRead<'a> {
    /// Get the page ID of the root header page
    fn get_header_page_id(&self) -> Result<PageId, HashTableHeaderExtensionError>;
    /// The next index to add a new entry
    fn get_next_ind(&self) -> Result<usize, HashTableHeaderExtensionError>
    where
        Self: Sized,
    {
        let mut next_ind = 0;
        while self.get_block_page_id(next_ind)?.is_some() {
            next_ind += 1;
            if next_ind >= Self::capacity_slots() {
                return Err(HashTableHeaderExtensionError::NoMoreCapacity);
            }
        }
        Ok(next_ind)
    }
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
    /// Iterate over block page IDs within this extension page
    fn iter_block_page_ids<'b>(&'b self) -> BlockPageIdIterator<'b, 'a>;
    /// Block page ID capacity slots
    fn capacity_slots() -> usize
    where
        Self: Sized,
    {
        BLOCK_PAGE_IDS_COUNT
    }
}

/// Interact with a page as a hash table header page.
pub trait IHashTableHeaderExtensionPageWrite<'a>: IHashTableHeaderExtensionPageRead<'a> {
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
    /// Higher-level function to add a block page ID to the header page in the next slot
    fn add_block_page_id(&mut self, page_id: PageId) -> Result<(), HashTableHeaderExtensionError>
    where
        Self: Sized,
    {
        self.set_block_page_id(self.get_next_ind()?, Some(page_id))?;
        Ok(())
    }
}

pub struct BlockPageIdIterator<'a, 'b> {
    header_page: &'a dyn IHashTableHeaderExtensionPageRead<'a>,
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

pub struct ReadOnlyHashTableHeaderExtensionPage<'a> {
    page: ReadOnlyPage<'a>,
}

impl<'a> ReadOnlyHashTableHeaderExtensionPage<'a> {
    pub fn new(page: ReadOnlyPage<'a>) -> Self {
        Self { page }
    }

    fn read_single_at_offset(
        &self,
        offset_bytes: usize,
    ) -> Result<u32, HashTableHeaderExtensionError> {
        let data = self.page.read_data(offset_bytes, PAGE_ENTRY_SIZE_BYTES)?;
        let result = u32::from_be_bytes(data.as_slice().try_into().unwrap());
        Ok(result)
    }
}

impl<'a> IHashTableHeaderExtensionPageRead<'a> for ReadOnlyHashTableHeaderExtensionPage<'a> {
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

    fn iter_block_page_ids<'b>(&'b self) -> BlockPageIdIterator<'b, 'a> {
        BlockPageIdIterator {
            header_page: self,
            current_position: 0,
            max_position: BLOCK_PAGE_IDS_COUNT,
            _lifetime: std::marker::PhantomData,
        }
    }
}

pub struct WritableHashTableHeaderExtensionPage<'a> {
    page: WritablePage<'a>,
}

impl<'a> WritableHashTableHeaderExtensionPage<'a> {
    pub fn new(page: WritablePage<'a>) -> Self {
        Self { page }
    }

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
    pub fn initialize(
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

impl<'a> IHashTableHeaderExtensionPageRead<'a> for WritableHashTableHeaderExtensionPage<'a> {
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

    fn iter_block_page_ids<'b>(&'b self) -> BlockPageIdIterator<'b, 'a> {
        BlockPageIdIterator {
            header_page: self,
            current_position: 0,
            max_position: BLOCK_PAGE_IDS_COUNT,
            _lifetime: std::marker::PhantomData,
        }
    }
}

impl<'a> IHashTableHeaderExtensionPageWrite<'a> for WritableHashTableHeaderExtensionPage<'a> {
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
    fn test_writable_extension_page_iter_block_page_ids() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        let mut ext_page = WritableHashTableHeaderExtensionPage { page };

        ext_page.initialize(999, None, None).unwrap();
        ext_page.set_block_page_id(100, Some(123)).unwrap();
        ext_page.set_block_page_id(101, Some(234)).unwrap();
        ext_page.set_block_page_id(140, Some(345)).unwrap();

        let mut iter = ext_page.iter_block_page_ids();
        let mut block_page_ids = Vec::new();
        while let Some(block_page_id) = iter.next() {
            block_page_ids.push(block_page_id);
        }

        assert_eq!(block_page_ids, vec![123, 234, 345]);
    }

    #[rstest]
    #[case(Some(123), Some(456))]
    #[case(Some(123), None)]
    #[case(None, Some(456))]
    #[case(None, None)]
    fn test_threaded_extension_page(
        #[case] prev_ext_page_id: Option<PageId>,
        #[case] next_ext_page_id: Option<PageId>,
    ) {
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

                        writer
                            .initialize(i, prev_ext_page_id, next_ext_page_id)
                            .unwrap();
                        writer.set_block_page_id(100, Some(i * 3)).unwrap();
                        writer.set_block_page_id(101, Some(999)).unwrap();
                        writer.set_block_page_id(101, None).unwrap();
                        writer.set_block_page_id(150, Some(111)).unwrap();
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

                        assert_eq!(
                            reader.get_previous_extension_page_id().unwrap(),
                            prev_ext_page_id
                        );
                    }
                    bpm.unpin_page(i, false).unwrap();
                }));
                let bpm = pool_manager.clone();
                read_threads.push(std::thread::spawn(move || {
                    {
                        let page = bpm.fetch_page(i).unwrap();
                        let reader = ReadOnlyHashTableHeaderExtensionPage { page };

                        assert_eq!(
                            reader.get_next_extension_page_id().unwrap(),
                            next_ext_page_id
                        );
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
                let bpm = pool_manager.clone();
                read_threads.push(std::thread::spawn(move || {
                    {
                        let page = bpm.fetch_page(i).unwrap();
                        let reader = ReadOnlyHashTableHeaderExtensionPage { page };

                        assert_eq!(reader.get_block_page_id(101).unwrap(), None);
                    }
                    bpm.unpin_page(i, false).unwrap();
                }));
                let bpm = pool_manager.clone();
                read_threads.push(std::thread::spawn(move || {
                    {
                        let page = bpm.fetch_page(i).unwrap();
                        let reader = ReadOnlyHashTableHeaderExtensionPage { page };

                        let mut iter = reader.iter_block_page_ids();
                        let mut block_page_ids = Vec::new();
                        while let Some(block_page_id) = iter.next() {
                            block_page_ids.push(block_page_id);
                        }

                        assert_eq!(block_page_ids, vec![i * 3, 111]);
                    }
                    bpm.unpin_page(i, false).unwrap();
                }));
            }
        }
    }
}
