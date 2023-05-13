use crate::dbms::{
    buffer::types::{ReadOnlyPage, WritablePage},
    storage::page::{PageError},
    types::PageId,
};

pub enum HashTableHeaderError {
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
pub trait IHashTableHeaderPage {
    /// Get the page ID
    fn get_page_id(&self, page: &ReadOnlyPage) -> Result<PageId, HashTableHeaderError>;
    /// Set the page ID
    fn set_page_id(
        &mut self,
        page: &mut WritablePage,
        page_id: PageId,
    ) -> Result<(), HashTableHeaderError>;
    /// Number of Key & Value pairs the hash table can hold
    fn get_size(&self, page: &ReadOnlyPage) -> Result<u32, HashTableHeaderError>;
    /// Set the number of Key & Value pairs the hash table can hold
    fn set_size(&mut self, page: &mut WritablePage, size: u32) -> Result<(), HashTableHeaderError>;
    /// The next index to add a new entry
    fn get_next_ind(&self, page: &ReadOnlyPage) -> Result<u32, HashTableHeaderError>;
    /// Set the next index to add a new entry
    fn set_next_ind(
        &mut self,
        page: &mut WritablePage,
        next_ind: u32,
    ) -> Result<(), HashTableHeaderError>;
    /// The log sequence number
    fn get_lsn(&self, page: &ReadOnlyPage) -> Result<u32, HashTableHeaderError>;
    /// Set the log sequence number
    fn set_lsn(&mut self, page: &mut WritablePage, lsn: u32) -> Result<(), HashTableHeaderError>;
    /// Get the page ID at the given index
    fn get_block_page_id(
        &self,
        page: &ReadOnlyPage,
        position: usize,
    ) -> Result<PageId, HashTableHeaderError>;
    /// Set the page ID at the given index
    fn set_block_page_id(
        &mut self,
        page: &mut WritablePage,
        position: usize,
        page_id: PageId,
    ) -> Result<(), HashTableHeaderError>;
}

pub struct HashTableHeaderPage;

impl HashTableHeaderPage {
    fn read_single_at_offset(
        &self,
        page: &ReadOnlyPage,
        offset_bytes: usize,
    ) -> Result<u32, HashTableHeaderError> {
        let data = page.read_data(offset_bytes, PAGE_ENTRY_SIZE_BYTES)?;
        let result = u32::from_be_bytes(data.as_slice().try_into().unwrap());
        Ok(result)
    }

    fn write_single_at_offset(
        &self,
        page: &mut WritablePage,
        offset_bytes: usize,
        value: u32,
    ) -> Result<(), HashTableHeaderError> {
        page.write_data(offset_bytes, &value.to_be_bytes())?;
        Ok(())
    }
}

impl IHashTableHeaderPage for HashTableHeaderPage {
    fn get_page_id(&self, page: &ReadOnlyPage) -> Result<PageId, HashTableHeaderError> {
        self.read_single_at_offset(page, PAGE_ID_OFFSET_BYTES)
    }

    fn set_page_id(
        &mut self,
        page: &mut WritablePage,
        page_id: PageId,
    ) -> Result<(), HashTableHeaderError> {
        self.write_single_at_offset(page, PAGE_ID_OFFSET_BYTES, page_id)
    }

    fn get_size(&self, page: &ReadOnlyPage) -> Result<u32, HashTableHeaderError> {
        self.read_single_at_offset(page, SIZE_OFFSET_BYTES)
    }

    fn set_size(&mut self, page: &mut WritablePage, size: u32) -> Result<(), HashTableHeaderError> {
        self.write_single_at_offset(page, SIZE_OFFSET_BYTES, size)
    }

    fn get_next_ind(&self, page: &ReadOnlyPage) -> Result<u32, HashTableHeaderError> {
        self.read_single_at_offset(page, NEXT_IND_OFFSET_BYTES)
    }

    fn set_next_ind(
        &mut self,
        page: &mut WritablePage,
        next_ind: u32,
    ) -> Result<(), HashTableHeaderError> {
        self.write_single_at_offset(page, NEXT_IND_OFFSET_BYTES, next_ind)
    }

    fn get_lsn(&self, page: &ReadOnlyPage) -> Result<u32, HashTableHeaderError> {
        self.read_single_at_offset(page, LSN_OFFSET_BYTES)
    }

    fn set_lsn(&mut self, page: &mut WritablePage, lsn: u32) -> Result<(), HashTableHeaderError> {
        self.write_single_at_offset(page, LSN_OFFSET_BYTES, lsn)
    }

    fn get_block_page_id(
        &self,
        page: &ReadOnlyPage,
        position: usize,
    ) -> Result<PageId, HashTableHeaderError> {
        self.read_single_at_offset(
            page,
            BLOCK_PAGE_IDS_START_OFFSET_BYTES + position * PAGE_ENTRY_SIZE_BYTES,
        )
    }

    fn set_block_page_id(
        &mut self,
        page: &mut WritablePage,
        position: usize,
        page_id: PageId,
    ) -> Result<(), HashTableHeaderError> {
        self.write_single_at_offset(
            page,
            BLOCK_PAGE_IDS_START_OFFSET_BYTES + position * PAGE_ENTRY_SIZE_BYTES,
            page_id,
        )
    }
}
