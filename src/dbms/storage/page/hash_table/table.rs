use crate::dbms::{
    buffer::{
        pool_manager::{BufferPoolManagerError, IBufferPoolManager},
        types::{ReadOnlyPage, WritablePage},
    },
    storage::serialize::BytesSerialize,
    types::PageId,
};

use super::header::{HashTableHeaderError, IHashTableHeaderPageRead, WritableHashTableHeaderPage};

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

pub trait IHashTable<KeyType: BytesSerialize, ValueType: BytesSerialize> {
    type KeyType;
    type ValueType;

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

impl<KeyType: BytesSerialize, ValueType: BytesSerialize> IHashTable<KeyType, ValueType>
    for LinearProbingHashTable
{
    type KeyType = KeyType;
    type ValueType = ValueType;

    fn initialize(
        pool: &mut impl IBufferPoolManager,
        initial_table_size: u32,
    ) -> Result<Self, HashTableError> {
        let header_page_id: u32;

        {
            // Init header page
            let page = pool.new_page()?;
            let mut header_page = WritableHashTableHeaderPage::new(page);
            header_page.initialize(initial_table_size)?;
            header_page_id = header_page.get_page_id()?;
        }

        // Init any extension pages needed for the size

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
