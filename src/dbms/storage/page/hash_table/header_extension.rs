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
    fn get_previous_extension_page(&self) -> Result<Option<PageId>, HashTableHeaderExtensionError>;
    /// Get the page ID of the next extension page, if there is one
    fn get_next_extension_page(&self) -> Result<Option<PageId>, HashTableHeaderExtensionError>;
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

    fn get_previous_extension_page(&self) -> Result<Option<PageId>, HashTableHeaderExtensionError> {
        match self.read_single_at_offset(PREVIOUS_EXTENSION_PAGE_OFFSET_BYTES)? {
            NULL_PAGE_ID => Ok(None),
            p => Ok(Some(p)),
        }
    }

    fn get_next_extension_page(&self) -> Result<Option<PageId>, HashTableHeaderExtensionError> {
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

    /// Initialize a header page to contain its page ID
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

    fn get_previous_extension_page(&self) -> Result<Option<PageId>, HashTableHeaderExtensionError> {
        match self.read_single_at_offset(PREVIOUS_EXTENSION_PAGE_OFFSET_BYTES)? {
            NULL_PAGE_ID => Ok(None),
            p => Ok(Some(p)),
        }
    }

    fn get_next_extension_page(&self) -> Result<Option<PageId>, HashTableHeaderExtensionError> {
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
