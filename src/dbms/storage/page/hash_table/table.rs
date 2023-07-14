use rand::Rng;
use std::marker::PhantomData;

use crate::dbms::{
    buffer::pool_manager::{BufferPoolManagerError, IBufferPoolManager},
    storage::{
        page::{
            hash_table::{
                block::{
                    IHashTableBlockPageWrite, ReadOnlyHashTableBlockPage,
                    WritableHashTableBlockPage,
                },
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
    hash_function::HashFunction,
    header::{HashTableHeaderError, IHashTableHeaderPageRead, WritableHashTableHeaderPage},
    header_extension::{
        HashTableHeaderExtensionError, IHashTableHeaderExtensionPageRead,
        WritableHashTableHeaderExtensionPage,
    },
    util::{calculate_block_page_layout, PageLayoutError},
};

#[derive(Debug, PartialEq, Eq)]
pub enum HashTableInsertResult {
    Inserted,
    DuplicateEntry,
}

#[derive(Debug, PartialEq, Eq)]
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

    fn get_address_from_hash(&self, offset: usize) -> Result<EntryAddress, HashTableError> {
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
        key: &KeyType,
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
        let new_extension_page_id;
        let mut mark_header_dirty = false;

        {
            let new_page = pool.new_page()?;
            new_extension_page_id = new_page.get_page_id()?.unwrap();
            let mut new_ext_page = WritableHashTableHeaderExtensionPage::new(new_page);
            new_ext_page.initialize(self.header_page_id, None, None)?;

            let mut header_page =
                WritableHashTableHeaderPage::new(pool.fetch_page_writable(self.header_page_id)?);
            let header_extension_page = header_page.get_extension_page_id()?;

            if let Some(ext_page_id) = header_extension_page {
                // There is already at least one extension page - find the last
                // extension page and add the new one to the end
                let mut last_ext_page = ext_page_id;

                loop {
                    let next_ext_page;

                    {
                        let page = pool.fetch_page_writable(last_ext_page)?;
                        let ext_page = WritableHashTableHeaderExtensionPage::new(page);
                        next_ext_page = ext_page.get_next_extension_page_id()?;
                    }

                    pool.unpin_page(last_ext_page, false)?;
                    match next_ext_page {
                        Some(next_page_id) => last_ext_page = next_page_id,
                        None => break,
                    }
                }

                // Set the new extentions page to point to the last one
                new_ext_page.set_previous_extension_page_id(Some(last_ext_page))?;

                // Update the last extension page to point to the new one
                {
                    let mut previous_ext_page = WritableHashTableHeaderExtensionPage::new(
                        pool.fetch_page_writable(last_ext_page)?,
                    );
                    previous_ext_page.set_next_extension_page_id(Some(new_extension_page_id))?;
                }

                pool.unpin_page(last_ext_page, true)?;
            } else {
                // No extension page exists yet - mark it in the header as the first.
                header_page.set_extension_page_id(Some(new_extension_page_id))?;
                mark_header_dirty = true;
            }
        }

        pool.unpin_page(self.header_page_id, mark_header_dirty)?;
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

                Ok(new_block_page_id)
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
                    let add_res;
                    {
                        let mut ext_page = WritableHashTableHeaderExtensionPage::new(
                            pool.fetch_page_writable(ext_page_id)?,
                        );
                        add_res = ext_page.add_block_page_id(new_block_page_id);
                        next_ext_page_res = ext_page.get_next_extension_page_id()?;
                    }
                    if add_res.is_ok() {
                        pool.unpin_page(ext_page_id, true)?;
                        return Ok(new_block_page_id);
                    } else {
                        pool.unpin_page(ext_page_id, false)?;
                    }
                }
                Err(HashTableError::NoSlotsInTable)
            }
            Err(e) => {
                pool.unpin_page(self.header_page_id, false)?;

                Err(HashTableError::HashTableHeaderError(e))
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
                        Ok(page_id)
                    } else {
                        panic!("No block page id found for slot {}", n);
                    }
                }
                Err(e) => Err(HashTableError::HashTableHeaderError(e)),
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
                                Ok(page_id)
                            } else {
                                panic!("No block page id found for slot {}", n);
                            }
                        }
                        Err(e) => Err(HashTableError::HashTableHeaderExtensionError(e)),
                    }
                } else {
                    panic!("Hash table is corrupt - extension page required but none found");
                }
            }
        }
    }

    fn double_table_size(
        &mut self,
        pool: &mut impl IBufferPoolManager,
    ) -> Result<(), HashTableError> {
        let tmp_header_page_id;
        let new_hash_seed;
        {
            let mut tmp_hash_table = Self::create(pool, (self.size(pool)? * 2) as u32)?;
            tmp_header_page_id = tmp_hash_table.header_page_id;
            new_hash_seed = tmp_hash_table.hash_seed;

            let mut all_block_page_ids = Vec::<PageId>::new();
            let mut next_extension_page_id: Option<PageId>;
            {
                let header_page =
                    ReadOnlyHashTableHeaderPage::new(pool.fetch_page(self.header_page_id)?);
                all_block_page_ids.extend(header_page.iter_block_page_ids().collect::<Vec<_>>());
                next_extension_page_id = header_page.get_extension_page_id()?;
            }
            pool.unpin_page(self.header_page_id, false)?;

            while let Some(extension_page_id) = next_extension_page_id {
                let extension_page;
                {
                    extension_page = ReadOnlyHashTableHeaderExtensionPage::new(
                        pool.fetch_page(extension_page_id)?,
                    );
                    all_block_page_ids
                        .extend(extension_page.iter_block_page_ids().collect::<Vec<_>>());
                    next_extension_page_id = extension_page.get_next_extension_page_id()?;
                }
                pool.unpin_page(extension_page_id, false)?;
            }

            all_block_page_ids
                .into_iter()
                .try_for_each::<_, Result<_, HashTableError>>(|block_page_id| {
                    let block_page_entries;
                    {
                        let block_page = ReadOnlyHashTableBlockPage::<KeyType, ValueType>::new(
                            pool.fetch_page(block_page_id)?,
                        );
                        block_page_entries = block_page.iter_entries(0, None)?.collect::<Vec<_>>();
                    }
                    pool.unpin_page(block_page_id, false)?;

                    block_page_entries
                        .into_iter()
                        .try_for_each::<_, Result<_, HashTableError>>(|entry| {
                            if let Some((key, value)) = entry.entry {
                                tmp_hash_table.insert_entry(pool, key, value)?;
                            }
                            Ok(())
                        })?;

                    Ok(())
                })?;
        }

        {
            let tmp_hash_table_header_page =
                ReadOnlyHashTableHeaderPage::new(pool.fetch_page(tmp_header_page_id)?);
            let tmp_hash_table_header_page_size = tmp_hash_table_header_page.get_size()?;
            let tmp_hash_table_header_page_lsn = tmp_hash_table_header_page.get_lsn()?;
            let tmp_hash_table_header_page_extension_page_id =
                tmp_hash_table_header_page.get_extension_page_id()?;
            let tmp_hash_table_header_page_block_page_ids =
                tmp_hash_table_header_page.iter_block_page_ids();

            let mut header_page =
                WritableHashTableHeaderPage::new(pool.fetch_page_writable(self.header_page_id)?);
            header_page.set_size(tmp_hash_table_header_page_size)?;
            header_page.set_lsn(tmp_hash_table_header_page_lsn)?;
            header_page.set_extension_page_id(tmp_hash_table_header_page_extension_page_id)?;
            (0..WritableHashTableHeaderPage::capacity_slots())
                .try_for_each::<_, Result<_, HashTableError>>(|i| {
                    header_page.set_block_page_id(i, None)?;
                    Ok(())
                })?;
            tmp_hash_table_header_page_block_page_ids
                .into_iter()
                .try_for_each::<_, Result<_, HashTableError>>(|block_page_id| {
                    header_page.add_block_page_id(block_page_id)?;
                    Ok(())
                })?;
        }
        pool.unpin_page(tmp_header_page_id, false)?;
        pool.unpin_page(self.header_page_id, true)?;

        self.hash_seed = new_hash_seed;

        Ok(())
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

    fn get_addressed_values(
        &self,
        pool: &impl IBufferPoolManager,
        key: &KeyType,
        max_returned: Option<usize>,
    ) -> Result<Vec<(EntryAddress, ValueType)>, HashTableError> {
        let hash = self.get_hash_from_key(pool, key)?;

        let table_size = self.size(pool)?;
        let max_address = self.get_address_from_hash(table_size - 1)?;

        let original_address = self.get_address_from_hash(hash)?;
        let mut address = original_address;
        let mut to_slot: Option<usize> = None;
        let mut wrapped = false;

        let mut results: Vec<(EntryAddress, ValueType)> = Vec::new();

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

                'entries: for (i, entry) in entries_iter.enumerate() {
                    if entry.occupied {
                        if let Some((k, v)) = entry.entry {
                            if k == *key {
                                let entry_address = EntryAddress {
                                    block_page_num: address.block_page_num,
                                    slot: address.slot + i,
                                };
                                results.push((entry_address, v));
                                if let Some(max_returned) = max_returned {
                                    if results.len() >= max_returned {
                                        search_done = true;
                                        break 'entries;
                                    }
                                }
                            }
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

        Ok(results)
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
        pool.unpin_page(header_page_id, true)?;

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

        Ok(res)
    }

    fn get_single_value(
        &self,
        pool: &impl IBufferPoolManager,
        key: KeyType,
    ) -> Result<Option<ValueType>, HashTableError> {
        let mut results = self.get_addressed_values(pool, &key, Some(1))?;
        if let Some(r) = results.pop() {
            Ok(Some(r.1))
        } else {
            Ok(None)
        }
    }

    fn get_all_values(
        &self,
        pool: &impl IBufferPoolManager,
        key: KeyType,
    ) -> Result<Vec<ValueType>, HashTableError> {
        Ok(self
            .get_addressed_values(pool, &key, None)?
            .into_iter()
            .map(|(_, v)| v)
            .collect())
    }

    fn insert_entry(
        &mut self,
        pool: &mut impl IBufferPoolManager,
        key: KeyType,
        value: ValueType,
    ) -> Result<HashTableInsertResult, HashTableError> {
        let hash = self.get_hash_from_key(pool, &key)?;

        let table_size = self.size(pool)?;
        let max_address = self.get_address_from_hash(table_size - 1)?;

        let original_address = self.get_address_from_hash(hash)?;
        let mut address = original_address;
        let mut to_slot: Option<usize> = None;
        let mut wrapped = false;

        let num_block_pages = self.num_block_pages(pool)?;

        let mut search_done = false;
        let mut slot_result: Option<usize> = None;
        let mut found_duplicate = false;
        while !search_done {
            let block_page_id = self.get_nth_block_page_id(pool, address.block_page_num)?;

            {
                let mut block_page = WritableHashTableBlockPage::<KeyType, ValueType>::new(
                    pool.fetch_page_writable(block_page_id)?,
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
                {
                    let entries_iter = block_page.iter_entries(address.slot, s)?;

                    'entries: for (i, entry) in entries_iter.enumerate() {
                        if let Some((k, v)) = entry.entry {
                            if k == key && v == value {
                                // Found this entry already in the table
                                search_done = true;
                                found_duplicate = true;
                                break 'entries;
                            }
                        }
                        if !entry.occupied {
                            // Found the first empty slot at this key's hash
                            search_done = true;
                            let slot = address.slot + i;
                            slot_result = Some(slot);
                            break 'entries;
                        }
                    }
                }

                if let Some(slot) = slot_result {
                    block_page.put_slot(slot, key.clone(), value.clone())?;
                }

                // We reached the end of the block page without finding an
                // empty slot, so we need to continue searching in the next
                // block page
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

        if found_duplicate {
            Ok(HashTableInsertResult::DuplicateEntry)
        } else if slot_result.is_some() {
            Ok(HashTableInsertResult::Inserted)
        } else {
            // Double the table size and try again
            self.double_table_size(pool)?;
            self.insert_entry(pool, key, value)
        }
    }

    fn delete_entry(
        &mut self,
        pool: &mut impl IBufferPoolManager,
        key: KeyType,
        value: ValueType,
    ) -> Result<HashTableDeleteResult, HashTableError> {
        let hash = self.get_hash_from_key(pool, &key)?;

        let table_size = self.size(pool)?;
        let max_address = self.get_address_from_hash(table_size - 1)?;

        let original_address = self.get_address_from_hash(hash)?;
        let mut address = original_address;
        let mut to_slot: Option<usize> = None;
        let mut wrapped = false;

        let num_block_pages = self.num_block_pages(pool)?;

        let mut search_done = false;
        let mut removed = false;
        while !search_done {
            let block_page_id = self.get_nth_block_page_id(pool, address.block_page_num)?;

            {
                let mut block_page = WritableHashTableBlockPage::<KeyType, ValueType>::new(
                    pool.fetch_page_writable(block_page_id)?,
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
                {
                    let entries_iter = block_page.iter_entries(address.slot, s)?;

                    'entries: for (i, entry) in entries_iter.enumerate() {
                        if let Some((k, v)) = entry.entry {
                            if k == key && v == value {
                                // Found a matching entry to delete
                                search_done = true;
                                let slot = address.slot + i;
                                block_page.remove_slot(slot)?;
                                removed = true;
                                break 'entries;
                            }
                        }
                    }
                }

                // We reached the end of the block page without finding an
                // empty slot, so we need to continue searching in the next
                // block page
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

        if removed {
            Ok(HashTableDeleteResult::Deleted)
        } else {
            Ok(HashTableDeleteResult::DidNotExist)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        dbms::{
            buffer::pool_manager::testing::create_testing_pool_manager,
            storage::page::hash_table::hash_function::{ConstHashFunction, XxHashFunction},
        },
        tuple, tuple_type,
    };
    use paste;
    use std::time::Duration;

    use super::*;
    use rstest::*;

    macro_rules! test_with_hash_fns {
        ($test_fn:ident) => {
            paste::item! {
                #[test]
                fn [< $test_fn _xx_hash >]() {
                    $test_fn::<XxHashFunction>();
                }

                #[test]
                fn [< $test_fn _const_hash >]() {
                    $test_fn::<ConstHashFunction>();
                }
            }
        };
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    /// Can create a hash table regardless of pool and table size
    fn test_create_hash_table(
        #[values(5, 100, 1000)] buffer_pool_size: usize,
        #[values(5, 100, 1_000, 10_000, 100_000, 1_000_000)] initial_table_size: u32,
    ) {
        let mut pool_manager = create_testing_pool_manager(buffer_pool_size);

        LinearProbingHashTable::<tuple_type![u32, bool], tuple_type![f64, u32, bool], XxHashFunction>::create(
            &mut pool_manager,
            initial_table_size,
        )
        .unwrap();
    }

    fn test_add_and_get_hash_table_value<T: HashFunction>() {
        let mut pool_manager = create_testing_pool_manager(100);

        let mut table = LinearProbingHashTable::<
            tuple_type![u32, bool],
            tuple_type![f64, u32, bool],
            T,
        >::create(&mut pool_manager, 1000)
        .unwrap();

        let insert_result = table
            .insert_entry(&mut pool_manager, tuple![1, true], tuple![1.0, 1, true])
            .unwrap();
        assert_eq!(insert_result, HashTableInsertResult::Inserted);

        let get_result = table
            .get_single_value(&pool_manager, tuple![1, true])
            .unwrap();
        assert_eq!(get_result, Some(tuple![1.0, 1, true]));
    }
    test_with_hash_fns!(test_add_and_get_hash_table_value);

    fn test_add_and_get_hash_table_values<T: HashFunction>() {
        let mut pool_manager = create_testing_pool_manager(100);

        let mut table = LinearProbingHashTable::<
            tuple_type![u32, bool],
            tuple_type![f64, u32, bool],
            XxHashFunction,
        >::create(&mut pool_manager, 1000)
        .unwrap();

        let insert_result = table
            .insert_entry(&mut pool_manager, tuple![1, true], tuple![1.0, 1, true])
            .unwrap();
        assert_eq!(insert_result, HashTableInsertResult::Inserted);
        let insert_result = table
            .insert_entry(&mut pool_manager, tuple![2, false], tuple![2.1, 2, true])
            .unwrap();
        assert_eq!(insert_result, HashTableInsertResult::Inserted);
        let insert_result = table
            .insert_entry(&mut pool_manager, tuple![3, true], tuple![3.2, 3, true])
            .unwrap();
        assert_eq!(insert_result, HashTableInsertResult::Inserted);

        let get_result = table
            .get_single_value(&pool_manager, tuple![1, true])
            .unwrap();
        assert_eq!(get_result, Some(tuple![1.0, 1, true]));
        let get_result = table
            .get_single_value(&pool_manager, tuple![2, false])
            .unwrap();
        assert_eq!(get_result, Some(tuple![2.1, 2, true]));
        let get_result = table
            .get_single_value(&pool_manager, tuple![3, true])
            .unwrap();
        assert_eq!(get_result, Some(tuple![3.2, 3, true]));
    }
    test_with_hash_fns!(test_add_and_get_hash_table_values);

    fn test_adding_many_values_increases_table_size<T: HashFunction>() {
        let mut pool_manager = create_testing_pool_manager(100);

        let mut table = LinearProbingHashTable::<
            tuple_type![u32, bool],
            tuple_type![f64, u32, bool],
            T,
        >::create(&mut pool_manager, 100)
        .unwrap();

        for i in 0..1000 {
            let insert_result = table
                .insert_entry(
                    &mut pool_manager,
                    tuple![i, true],
                    tuple![i as f64, i, true],
                )
                .unwrap();
            assert_eq!(insert_result, HashTableInsertResult::Inserted);
        }

        assert_eq!(table.size(&pool_manager).unwrap(), 1600);

        for i in 0..1000 {
            let get_result = table
                .get_single_value(&pool_manager, tuple![i, true])
                .unwrap();
            assert_eq!(get_result, Some(tuple![i as f64, i, true]));
        }
    }
    test_with_hash_fns!(test_adding_many_values_increases_table_size);

    fn test_remove_entries<T: HashFunction>() {
        let mut pool_manager = create_testing_pool_manager(100);

        let mut table = LinearProbingHashTable::<
            tuple_type![u32, bool],
            tuple_type![f64, u32, bool],
            T,
        >::create(&mut pool_manager, 100)
        .unwrap();

        for i in 0..1000 {
            let insert_result = table
                .insert_entry(
                    &mut pool_manager,
                    tuple![i, true],
                    tuple![i as f64, i, true],
                )
                .unwrap();
            assert_eq!(insert_result, HashTableInsertResult::Inserted);
        }

        for i in 0..500 {
            let remove_result = table
                .delete_entry(
                    &mut pool_manager,
                    tuple![i * 2, true],
                    tuple![(i * 2) as f64, i * 2, true],
                )
                .unwrap();
            assert_eq!(remove_result, HashTableDeleteResult::Deleted);
        }

        for i in 0..500 {
            let get_result = table
                .get_single_value(&pool_manager, tuple![i * 2, true])
                .unwrap();
            assert_eq!(get_result, None);
            let get_result = table
                .get_single_value(&pool_manager, tuple![i * 2 + 1, true])
                .unwrap();
            assert_eq!(
                get_result,
                Some(tuple![(i * 2 + 1) as f64, i * 2 + 1, true])
            );
        }
    }
    test_with_hash_fns!(test_remove_entries);

    fn test_remove_entry_does_not_exist<T: HashFunction>() {
        let mut pool_manager = create_testing_pool_manager(100);

        let mut table = LinearProbingHashTable::<
            tuple_type![u32, bool],
            tuple_type![f64, u32, bool],
            T,
        >::create(&mut pool_manager, 100)
        .unwrap();

        assert_eq!(
            table
                .insert_entry(&mut pool_manager, tuple![1, true], tuple![1.0, 1, true])
                .unwrap(),
            HashTableInsertResult::Inserted
        );

        assert_eq!(
            table
                .delete_entry(&mut pool_manager, tuple![2, true], tuple![2.0, 2, true])
                .unwrap(),
            HashTableDeleteResult::DidNotExist
        );

        assert_eq!(
            table
                .get_single_value(&mut pool_manager, tuple![1, true])
                .unwrap(),
            Some(tuple![1.0, 1, true])
        );

        assert_eq!(
            table
                .get_single_value(&mut pool_manager, tuple![2, true])
                .unwrap(),
            None
        );
    }
    test_with_hash_fns!(test_remove_entry_does_not_exist);

    fn test_key_with_multiple_values<T: HashFunction>() {
        let mut pool_manager = create_testing_pool_manager(100);

        let mut table = LinearProbingHashTable::<
            tuple_type![u32, bool],
            tuple_type![f64, u32, bool],
            T,
        >::create(&mut pool_manager, 100)
        .unwrap();

        assert_eq!(
            table
                .insert_entry(&mut pool_manager, tuple![1, true], tuple![1.0, 1, true])
                .unwrap(),
            HashTableInsertResult::Inserted
        );
        assert_eq!(
            table
                .insert_entry(&mut pool_manager, tuple![1, true], tuple![2.0, 2, true])
                .unwrap(),
            HashTableInsertResult::Inserted
        );
        assert_eq!(
            table
                .insert_entry(&mut pool_manager, tuple![1, true], tuple![3.0, 3, false])
                .unwrap(),
            HashTableInsertResult::Inserted
        );

        assert_eq!(
            table
                .get_all_values(&mut pool_manager, tuple![1, true])
                .unwrap(),
            vec![
                tuple![1.0, 1, true],
                tuple![2.0, 2, true],
                tuple![3.0, 3, false]
            ]
        );

        assert_eq!(
            table
                .delete_entry(&mut pool_manager, tuple![1, true], tuple![2.0, 2, true])
                .unwrap(),
            HashTableDeleteResult::Deleted
        );

        assert_eq!(
            table
                .get_all_values(&mut pool_manager, tuple![1, true])
                .unwrap(),
            vec![tuple![1.0, 1, true], tuple![3.0, 3, false]]
        );
    }
    test_with_hash_fns!(test_key_with_multiple_values);

    fn test_insert_duplicate_value<T: HashFunction>() {
        let mut pool_manager = create_testing_pool_manager(100);

        let mut table = LinearProbingHashTable::<
            tuple_type![u32, bool],
            tuple_type![f64, u32, bool],
            T,
        >::create(&mut pool_manager, 100)
        .unwrap();

        assert_eq!(
            table
                .insert_entry(&mut pool_manager, tuple![1, true], tuple![1.0, 1, true])
                .unwrap(),
            HashTableInsertResult::Inserted
        );

        assert_eq!(
            table
                .insert_entry(&mut pool_manager, tuple![1, true], tuple![1.0, 1, true])
                .unwrap(),
            HashTableInsertResult::DuplicateEntry
        );

        assert_eq!(
            table
                .get_all_values(&mut pool_manager, tuple![1, true])
                .unwrap(),
            vec![tuple![1.0, 1, true]]
        );
    }
    test_with_hash_fns!(test_insert_duplicate_value);
}
