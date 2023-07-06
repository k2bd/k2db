use crate::dbms::{
    buffer::{
        pool_manager::{BufferPoolManagerError, IBufferPoolManager},
        types::{ReadOnlyPage, WritablePage},
    },
    storage::{
        page::{
            hash_table::{
                block::WritableHashTableBlockPage, header::IHashTableHeaderPageWrite,
                header_extension::IHashTableHeaderExtensionPageWrite,
            },
            PageError,
        },
        serialize::BytesSerialize,
    },
    types::PageId,
};

use super::{
    header::{HashTableHeaderError, IHashTableHeaderPageRead, WritableHashTableHeaderPage},
    header_extension::{
        HashTableHeaderExtensionError, IHashTableHeaderExtensionPageRead,
        WritableHashTableHeaderExtensionPage,
    },
};

pub enum HashTableInsertResult {
    Inserted,
    DuplicateEntry,
}
pub enum HashTableDeleteResult {
    Deleted,
    DidNotExist,
}
pub enum HashTableError {
    BufferPoolManagerError(BufferPoolManagerError),
    HashTableHeaderError(HashTableHeaderError),
    HashTableHeaderExtensionError(HashTableHeaderExtensionError),
    PageError(PageError),
}

impl From<BufferPoolManagerError> for HashTableError {
    fn from(error: BufferPoolManagerError) -> Self {
        HashTableError::BufferPoolManagerError(error)
    }
}

impl From<HashTableHeaderError> for HashTableError {
    fn from(error: HashTableHeaderError) -> Self {
        HashTableError::HashTableHeaderError(error)
    }
}

impl From<HashTableHeaderExtensionError> for HashTableError {
    fn from(error: HashTableHeaderExtensionError) -> Self {
        HashTableError::HashTableHeaderExtensionError(error)
    }
}

impl From<PageError> for HashTableError {
    fn from(error: PageError) -> Self {
        HashTableError::PageError(error)
    }
}

pub trait IHashTable<KeyType: BytesSerialize, ValueType: BytesSerialize> {
    /// Create a new hash table, initializing a new hash table header page
    fn initialize(
        pool: &mut impl IBufferPoolManager,
        initial_table_size: u32,
    ) -> Result<Self, HashTableError>
    where
        Self: Sized;

    /// Get a single tuple value with the given key
    fn get_single_value(
        &self,
        pool: &impl IBufferPoolManager,
        key: KeyType,
    ) -> Result<Option<ValueType>, HashTableError>;

    /// Get all values with the given key
    fn get_all_values(
        &self,
        pool: &impl IBufferPoolManager,
        key: KeyType,
    ) -> Result<Vec<ValueType>, HashTableError>;

    /// Insert a new entry
    fn insert_entry(
        &mut self,
        pool: &mut impl IBufferPoolManager,
        key: KeyType,
        value: ValueType,
    ) -> Result<HashTableInsertResult, HashTableError>;

    /// Delete an entry if it exists
    fn delete_entry(
        &mut self,
        pool: &mut impl IBufferPoolManager,
        key: KeyType,
        value: ValueType,
    ) -> Result<HashTableDeleteResult, HashTableError>;
}

pub struct LinearProbingHashTable {
    header_page_id: PageId,
}

impl LinearProbingHashTable {
    /// Create a new hash table extension page, updating any other pages to
    /// point to it as needed
    fn add_hash_table_extension_page(
        &self,
        pool: &mut impl IBufferPoolManager,
    ) -> Result<PageId, HashTableError> {
        let new_page = pool.new_page()?;
        let new_extension_page_id = new_page.get_page_id()?.unwrap();
        let mut new_ext_page = WritableHashTableHeaderExtensionPage::new(new_page);

        let mut header_page =
            WritableHashTableHeaderPage::new(pool.fetch_page_writable(self.header_page_id)?);
        let header_extension_page = header_page.get_extension_page_id()?;

        if let Some(ext_page_id) = header_extension_page {
            // There is already at least one extension page - find the last
            // extension page and add the new one to the end
            let mut last_ext_page = ext_page_id;
            loop {
                let page = pool.fetch_page_writable(last_ext_page)?;
                let ext_page = WritableHashTableHeaderExtensionPage::new(page);
                let next_ext_page = ext_page.get_next_extension_page_id()?;
                pool.unpin_page(last_ext_page, false)?;
                match next_ext_page {
                    Some(next_page_id) => last_ext_page = next_page_id,
                    None => break,
                }
            }

            // Create the new extension page
            new_ext_page.set_previous_extension_page_id(Some(last_ext_page))?;

            // Update the last extension page to point to the new one
            let mut previous_ext_page =
                WritableHashTableHeaderExtensionPage::new(pool.fetch_page_writable(last_ext_page)?);
            previous_ext_page.set_next_extension_page_id(Some(new_extension_page_id))?;

            pool.unpin_page(last_ext_page, true)?;
            pool.unpin_page(self.header_page_id, false)?;
        } else {
            // No extension page exists yet - mark it in the header as the first.
            header_page.set_extension_page_id(Some(new_extension_page_id))?;
            pool.unpin_page(self.header_page_id, true)?;
        }
        pool.unpin_page(new_extension_page_id, true)?;

        Ok(new_extension_page_id)
    }

    /// Create a new block page and add it to the header page
    fn add_block_page<KeyType: BytesSerialize, ValueType: BytesSerialize>(
        &self,
        pool: &mut impl IBufferPoolManager,
    ) -> Result<PageId, HashTableError> {
        let new_page = pool.new_page()?;
        let new_block_page_id = new_page.get_page_id()?.unwrap();
        let mut new_block_page = WritableHashTableBlockPage::<KeyType, ValueType>::new(new_page);

        let mut header_page =
            WritableHashTableHeaderPage::new(pool.fetch_page_writable(self.header_page_id)?);

        Ok(new_block_page_id)
    }
}

fn pages_required_for_slot(page_capacity: usize, slots: usize) -> usize {
    let mut pages = slots / page_capacity;
    if slots % page_capacity != 0 {
        pages += 1;
    }
    pages
}

impl<KeyType: BytesSerialize, ValueType: BytesSerialize> IHashTable<KeyType, ValueType>
    for LinearProbingHashTable
{
    fn initialize(
        pool: &mut impl IBufferPoolManager,
        initial_table_size: u32,
    ) -> Result<Self, HashTableError> {
        let header_page_id: u32;

        let res = Self { header_page_id: 0 };

        {
            // Init header page
            let page = pool.new_page()?;
            let mut header_page = WritableHashTableHeaderPage::new(page);
            header_page.initialize(initial_table_size)?;
            header_page_id = header_page.get_page_id()?;
        }

        let extension_slots_needed =
            initial_table_size as usize - WritableHashTableHeaderPage::capacity_slots();
        let extension_pages_needed = pages_required_for_slot(
            WritableHashTableHeaderExtensionPage::capacity_slots(),
            extension_slots_needed,
        );
        for _ in 0..extension_pages_needed {
            res.add_hash_table_extension_page(pool)?;
        }

        // Init enough block pages for the size

        pool.unpin_page(header_page_id, true)?;
        Ok(Self { header_page_id })
    }

    fn get_single_value(
        &self,
        pool: &impl IBufferPoolManager,
        key: KeyType,
    ) -> Result<Option<ValueType>, HashTableError> {
        todo!()
    }

    fn get_all_values(
        &self,
        pool: &impl IBufferPoolManager,
        key: KeyType,
    ) -> Result<Vec<ValueType>, HashTableError> {
        todo!()
    }

    fn insert_entry(
        &mut self,
        pool: &mut impl IBufferPoolManager,
        key: KeyType,
        value: ValueType,
    ) -> Result<HashTableInsertResult, HashTableError> {
        todo!()
    }

    fn delete_entry(
        &mut self,
        pool: &mut impl IBufferPoolManager,
        key: KeyType,
        value: ValueType,
    ) -> Result<HashTableDeleteResult, HashTableError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;
}
