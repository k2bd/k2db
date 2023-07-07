use std::marker::PhantomData;

use crate::dbms::{
    buffer::pool_manager::{BufferPoolManagerError, IBufferPoolManager},
    storage::{
        page::{
            hash_table::{
                header::IHashTableHeaderPageWrite,
                header_extension::IHashTableHeaderExtensionPageWrite,
            },
            PageError,
        },
        serialize::BytesSerialize,
    },
    types::PageId,
};

use super::{
    block::IHashTableBlockPageRead,
    header::{HashTableHeaderError, IHashTableHeaderPageRead, WritableHashTableHeaderPage},
    header_extension::{
        HashTableHeaderExtensionError, IHashTableHeaderExtensionPageRead,
        WritableHashTableHeaderExtensionPage,
    },
    util::{calculate_block_page_layout, PageLayoutError},
};

pub enum HashTableInsertResult {
    Inserted,
    DuplicateEntry,
}
pub enum HashTableDeleteResult {
    Deleted,
    DidNotExist,
}

#[derive(Debug)]
pub enum HashTableError {
    NoSlotsInTable,
    BufferPoolManagerError(BufferPoolManagerError),
    HashTableHeaderError(HashTableHeaderError),
    HashTableHeaderExtensionError(HashTableHeaderExtensionError),
    PageLayoutError(PageLayoutError),
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

impl From<PageLayoutError> for HashTableError {
    fn from(e: PageLayoutError) -> Self {
        HashTableError::PageLayoutError(e)
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

pub struct LinearProbingHashTable<KeyType: BytesSerialize, ValueType: BytesSerialize> {
    header_page_id: PageId,

    _key_type: std::marker::PhantomData<KeyType>,
    _value_type: std::marker::PhantomData<ValueType>,
}

impl<KeyType: BytesSerialize, ValueType: BytesSerialize>
    LinearProbingHashTable<KeyType, ValueType>
{
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

    /// Create a new block page and add it to the header page or an extension
    /// as needed
    fn add_block_page(&self, pool: &mut impl IBufferPoolManager) -> Result<PageId, HashTableError> {
        let new_block_page_id;
        {
            let new_page = pool.new_page()?;
            new_block_page_id = new_page.get_page_id()?.unwrap();
            pool.unpin_page(new_block_page_id, true)?;
        }

        let mut header_page =
            WritableHashTableHeaderPage::new(pool.fetch_page_writable(self.header_page_id)?);

        match header_page.add_block_page_id(new_block_page_id) {
            Ok(_) => {
                pool.unpin_page(self.header_page_id, true)?;
                return Ok(new_block_page_id);
            }
            Err(HashTableHeaderError::NoMoreCapacity) => {
                // Find the first extension page with space
                let mut next_ext_page_res = header_page.get_extension_page_id()?;
                while let Some(ext_page_id) = next_ext_page_res {
                    let mut ext_page = WritableHashTableHeaderExtensionPage::new(
                        pool.fetch_page_writable(ext_page_id)?,
                    );
                    if let Ok(_) = ext_page.add_block_page_id(new_block_page_id) {
                        pool.unpin_page(ext_page_id, true)?;
                        pool.unpin_page(self.header_page_id, false)?;
                        return Ok(new_block_page_id);
                    } else {
                        next_ext_page_res = ext_page.get_next_extension_page_id()?;
                        pool.unpin_page(ext_page_id, false)?;
                    }
                }
                pool.unpin_page(self.header_page_id, false)?;
                return Err(HashTableError::NoSlotsInTable);
            }
            Err(e) => {
                pool.unpin_page(self.header_page_id, false)?;
                return Err(HashTableError::HashTableHeaderError(e));
            }
        }
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
    for LinearProbingHashTable<KeyType, ValueType>
{
    fn initialize(
        pool: &mut impl IBufferPoolManager,
        initial_table_size: u32,
    ) -> Result<Self, HashTableError> {
        let header_page_id: u32;

        let res = Self {
            header_page_id: 0,
            _key_type: PhantomData,
            _value_type: PhantomData,
        };

        {
            // Init header page
            let page = pool.new_page()?;
            let mut header_page = WritableHashTableHeaderPage::new(page);
            header_page.initialize(initial_table_size)?;
            header_page_id = header_page.get_page_id()?;
        }

        let block_page_size =
            calculate_block_page_layout(KeyType::serialized_size() + ValueType::serialized_size())?
                .max_values;
        let block_pages_needed =
            pages_required_for_slot(block_page_size, initial_table_size as usize);

        if block_pages_needed > WritableHashTableHeaderPage::capacity_slots() {
            let extension_slots_needed =
                block_pages_needed - WritableHashTableHeaderPage::capacity_slots();
            let extension_pages_needed = pages_required_for_slot(
                WritableHashTableHeaderExtensionPage::capacity_slots(),
                extension_slots_needed,
            );
            for _ in 0..extension_pages_needed {
                res.add_hash_table_extension_page(pool)?;
            }
        }

        for _ in 0..block_pages_needed {
            res.add_block_page(pool)?;
        }

        {
            pool.unpin_page(header_page_id, true)?;
        }
        Ok(res)
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
    use crate::{dbms::buffer::pool_manager::testing::create_testing_pool_manager, tuple_type};

    use super::*;
    use rstest::*;

    #[rstest]
    #[case(100, 100)]
    #[case(5, 100)]
    #[case(100, 5)]
    #[case(100, 10000)]
    fn test_initialize(#[case] buffer_pool_size: usize, #[case] initial_table_size: u32) {
        let mut pool_manager = create_testing_pool_manager(buffer_pool_size);

        let mut hash_table = LinearProbingHashTable::<
            tuple_type![u32, bool],
            tuple_type![f64, u32, bool],
        >::initialize(&mut pool_manager, initial_table_size)
        .unwrap();
    }
}
