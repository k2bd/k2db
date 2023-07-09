use rand::Rng;
use std::{collections::hash_map::Entry, marker::PhantomData};

use crate::dbms::{
    buffer::pool_manager::{BufferPoolManagerError, IBufferPoolManager},
    storage::{
        page::{
            hash_table::{
                block::ReadOnlyHashTableBlockPage,
                header::{IHashTableHeaderPageWrite, ReadOnlyHashTableHeaderPage},
                header_extension::{
                    IHashTableHeaderExtensionPageWrite, ReadOnlyHashTableHeaderExtensionPage,
                },
            },
            PageError,
        },
        serialize::{BytesSerialize, SerializeError},
    },
    types::PageId,
};

use super::{
    block::{HashTableBlockError, IHashTableBlockPageRead},
    hash_function::{HashFunction, XxHashFunction},
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
    HashTableBlockError(HashTableBlockError),
    PageLayoutError(PageLayoutError),
    PageError(PageError),
    SerializeError(SerializeError),
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

impl From<HashTableBlockError> for HashTableError {
    fn from(error: HashTableBlockError) -> Self {
        HashTableError::HashTableBlockError(error)
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

impl From<SerializeError> for HashTableError {
    fn from(e: SerializeError) -> Self {
        HashTableError::SerializeError(e)
    }
}

pub trait IHashTable<KeyType: BytesSerialize, ValueType: BytesSerialize, HashFn: HashFunction> {
    /// Create a new hash table, initializing a new hash table header page
    fn create(
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

pub struct LinearProbingHashTable<
    KeyType: BytesSerialize,
    ValueType: BytesSerialize,
    HashFn: HashFunction,
> {
    header_page_id: PageId,
    hash_seed: u64,

    _key_type: std::marker::PhantomData<KeyType>,
    _value_type: std::marker::PhantomData<ValueType>,
    _hash_fn: std::marker::PhantomData<HashFn>,
}

#[derive(Clone, Copy)]
struct EntryAddress {
    block_page_num: usize,
    slot: usize,
}

impl<KeyType: BytesSerialize, ValueType: BytesSerialize, HashFn: HashFunction>
    LinearProbingHashTable<KeyType, ValueType, HashFn>
{
    fn new(header_page_id: PageId, hash_seed: u64) -> Self {
        Self {
            header_page_id,
            hash_seed,
            _key_type: PhantomData,
            _value_type: PhantomData,
            _hash_fn: PhantomData,
        }
    }

    fn get_address_from_hash(
        &self,
        pool: &impl IBufferPoolManager,
        offset: usize,
    ) -> Result<EntryAddress, HashTableError> {
        let block_page_size =
            calculate_block_page_layout(KeyType::serialized_size() + ValueType::serialized_size())?
                .max_values;

        let block_page_num = offset / block_page_size;
        let slot_within_block_page = offset % block_page_size;

        Ok(EntryAddress {
            block_page_num,
            slot: slot_within_block_page,
        })
    }

    fn get_hash_from_key(
        &self,
        pool: &impl IBufferPoolManager,
        key: KeyType,
    ) -> Result<usize, HashTableError> {
        let hash_fn = HashFn::new(self.hash_seed);
        let table_size;

        {
            let header_page =
                ReadOnlyHashTableHeaderPage::new(pool.fetch_page(self.header_page_id)?);
            table_size = header_page.get_size()?;
        }
        pool.unpin_page(self.header_page_id, false)?;

        let serialized_key = key.to_bytes()?;
        Ok(hash_fn.hash(&serialized_key, table_size.try_into().unwrap()))
    }

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
        }

        pool.unpin_page(new_block_page_id, true)?;

        let added_block_page_res;
        {
            let mut header_page =
                WritableHashTableHeaderPage::new(pool.fetch_page_writable(self.header_page_id)?);
            added_block_page_res = header_page.add_block_page_id(new_block_page_id);
        }
        pool.unpin_page(self.header_page_id, true)?;

        match added_block_page_res {
            Ok(_) => {
                pool.unpin_page(self.header_page_id, true)?;

                return Ok(new_block_page_id);
            }
            Err(HashTableHeaderError::NoMoreCapacity) => {
                // Find the first extension page with space
                let mut next_ext_page_res;
                {
                    let header_page =
                        ReadOnlyHashTableHeaderPage::new(pool.fetch_page(self.header_page_id)?);
                    next_ext_page_res = header_page.get_extension_page_id()?;
                }
                pool.unpin_page(self.header_page_id, false)?;

                while let Some(ext_page_id) = next_ext_page_res {
                    let mut ext_page = WritableHashTableHeaderExtensionPage::new(
                        pool.fetch_page_writable(ext_page_id)?,
                    );
                    if let Ok(_) = ext_page.add_block_page_id(new_block_page_id) {
                        pool.unpin_page(ext_page_id, true)?;
                        return Ok(new_block_page_id);
                    } else {
                        next_ext_page_res = ext_page.get_next_extension_page_id()?;
                        pool.unpin_page(ext_page_id, false)?;
                    }
                }
                return Err(HashTableError::NoSlotsInTable);
            }
            Err(e) => {
                return Err(HashTableError::HashTableHeaderError(e));
            }
        }
    }

    /// Get the n'th block page id in the hash table, either from the header
    /// page or an extension page
    fn get_nth_block_page_id(
        &self,
        pool: &impl IBufferPoolManager,
        n: usize,
    ) -> Result<PageId, HashTableError> {
        if n < ReadOnlyHashTableHeaderPage::capacity_slots() {
            let block_page_res;

            {
                let header_page =
                    ReadOnlyHashTableHeaderPage::new(pool.fetch_page(self.header_page_id)?);
                block_page_res = header_page.get_block_page_id(n);
            }
            pool.unpin_page(self.header_page_id, false)?;

            match block_page_res {
                Ok(page_id) => {
                    if let Some(page_id) = page_id {
                        return Ok(page_id);
                    } else {
                        panic!("No block page id found for slot {}", n);
                    }
                }
                Err(e) => {
                    return Err(HashTableError::HashTableHeaderError(e));
                }
            }
        } else {
            let n = n - ReadOnlyHashTableHeaderPage::capacity_slots();
            let extension_page_required =
                n / ReadOnlyHashTableHeaderExtensionPage::capacity_slots();
            let slot_within_page = n % ReadOnlyHashTableHeaderExtensionPage::capacity_slots();

            let mut next_ext_page_res;
            {
                let header_page =
                    ReadOnlyHashTableHeaderPage::new(pool.fetch_page(self.header_page_id)?);
                next_ext_page_res = header_page.get_extension_page_id()?;
            }
            pool.unpin_page(self.header_page_id, false)?;

            for _ in 0..extension_page_required {
                match next_ext_page_res {
                    Some(ext_page_id) => {
                        {
                            let ext_page = ReadOnlyHashTableHeaderExtensionPage::new(
                                pool.fetch_page(ext_page_id)?,
                            );
                            next_ext_page_res = ext_page.get_next_extension_page_id()?;
                        }
                        pool.unpin_page(ext_page_id, false)?;
                    }
                    None => {
                        panic!("Hash table is corrupt - extension page required but none found");
                    }
                }
            }

            {
                if let Some(ext_page_id) = next_ext_page_res {
                    let block_page_res;
                    {
                        let ext_page = ReadOnlyHashTableHeaderExtensionPage::new(
                            pool.fetch_page(ext_page_id)?,
                        );
                        block_page_res = ext_page.get_block_page_id(slot_within_page);
                    }
                    pool.unpin_page(ext_page_id, false)?;

                    match block_page_res {
                        Ok(page_id) => {
                            if let Some(page_id) = page_id {
                                return Ok(page_id);
                            } else {
                                panic!("No block page id found for slot {}", n);
                            }
                        }
                        Err(e) => {
                            return Err(HashTableError::HashTableHeaderExtensionError(e));
                        }
                    }
                } else {
                    panic!("Hash table is corrupt - extension page required but none found");
                }
            }
        }
    }

    fn double_table_size(&self) {
        todo!()
    }

    fn size(&self, pool: &impl IBufferPoolManager) -> Result<usize, HashTableError> {
        let result;
        {
            let header_page =
                ReadOnlyHashTableHeaderPage::new(pool.fetch_page(self.header_page_id)?);
            result = header_page.get_size()?;
        }
        pool.unpin_page(self.header_page_id, false)?;

        Ok(result as usize)
    }

    fn num_block_pages(&self, pool: &impl IBufferPoolManager) -> Result<usize, HashTableError> {
        num_block_pages_for_size::<KeyType, ValueType>(self.size(pool)?)
    }
}

fn pages_required_for_slot(page_capacity: usize, slots: usize) -> usize {
    let mut pages = slots / page_capacity;
    if slots % page_capacity != 0 {
        pages += 1;
    }
    pages
}

fn num_block_pages_for_size<KeyType: BytesSerialize, ValueType: BytesSerialize>(
    table_size: usize,
) -> Result<usize, HashTableError> {
    let block_page_size =
        calculate_block_page_layout(KeyType::serialized_size() + ValueType::serialized_size())?
            .max_values;
    Ok(pages_required_for_slot(block_page_size, table_size))
}

impl<KeyType: BytesSerialize, ValueType: BytesSerialize, HashFn: HashFunction>
    IHashTable<KeyType, ValueType, HashFn> for LinearProbingHashTable<KeyType, ValueType, HashFn>
{
    fn create(
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

        let seed = rand::thread_rng().gen();
        let res = Self::new(header_page_id, seed);

        let block_pages_needed = res.num_block_pages(pool)?;

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
        let hash = self.get_hash_from_key(pool, key)?;

        let table_size = self.size(pool)?;
        let max_address = self.get_address_from_hash(pool, table_size - 1)?;

        let original_address = self.get_address_from_hash(pool, hash)?;
        let mut address = original_address.clone();
        let mut to_slot: Option<usize> = None;
        let mut wrapped = false;

        let mut result: Option<ValueType> = None;

        let num_block_pages = self.num_block_pages(pool)?;

        let mut search_done = false;
        while !search_done {
            let block_page_id = self.get_nth_block_page_id(pool, address.block_page_num)?;

            {
                let block_page = ReadOnlyHashTableBlockPage::<KeyType, ValueType>::new(
                    pool.fetch_page(block_page_id)?,
                );
                let s = match (
                    to_slot,
                    original_address.block_page_num == address.block_page_num,
                ) {
                    (None, true) => Some(original_address.slot),
                    (None, false) => None,
                    (Some(t), true) => Some(t.min(max_address.slot)),
                    (Some(t), false) => Some(t),
                };
                let entries_iter = block_page.iter_entries(address.slot, s)?;

                'entries: for entry in entries_iter {
                    if let Some((key, value)) = entry {
                        if key == key {
                            result = Some(value);
                            search_done = true;
                            break 'entries;
                        }
                    } else {
                        // We reached an empty slot, so we can stop searching
                        search_done = true;
                        break 'entries;
                    }
                }

                // We reached the end of the block page without finding the key
                // so we need to continue searching in the next block page
                address = EntryAddress {
                    block_page_num: (address.block_page_num + 1) % num_block_pages,
                    slot: 0,
                };
                if wrapped {
                    search_done = true;
                } else if address.block_page_num == original_address.block_page_num {
                    to_slot = Some(original_address.slot);
                    wrapped = true;
                };
            }
            pool.unpin_page(block_page_id, false)?;
        }

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
    use crate::{
        dbms::{
            buffer::pool_manager::testing::create_testing_pool_manager,
            storage::page::hash_table::hash_function::ConstHashFunction,
        },
        tuple_type,
    };

    use super::*;
    use rstest::*;

    #[rstest]
    /// Can create a hash table regardless of pool and table size
    fn test_create_hash_table(
        #[values(2, 5, 100, 1000)] buffer_pool_size: usize,
        #[values(2, 5, 100, 1_000, 10_000, 100_000, 1_000_000)] initial_table_size: u32,
    ) {
        let mut pool_manager = create_testing_pool_manager(buffer_pool_size);

        LinearProbingHashTable::<tuple_type![u32, bool], tuple_type![f64, u32, bool], XxHashFunction>::create(
            &mut pool_manager,
            initial_table_size,
        )
        .unwrap();
    }

    #[rstest]
    fn test_add_and_get_hash_table_values_xxhash() {}
}
