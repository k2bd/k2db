use std::marker::PhantomData;

use crate::dbms::{
    buffer::types::{ReadOnlyPage, WritablePage},
    storage::{
        page::PageError,
        serialize::{BytesSerialize, SerializeError},
    },
};

use super::util::{calculate_block_page_layout, PageLayout, PageLayoutError};

pub trait IHashTableBlockPageRead<'a, KeyType: BytesSerialize, ValueType: BytesSerialize> {
    fn key_at(&self, slot: usize) -> Result<KeyType, HashTableBlockError>;
    fn value_at(&self, slot: usize) -> Result<ValueType, HashTableBlockError>;
    fn slot_occupied(&self, slot: usize) -> Result<bool, HashTableBlockError>;
    fn slot_readable(&self, slot: usize) -> Result<bool, HashTableBlockError>;
    fn num_slots(&self) -> Result<usize, HashTableBlockError> {
        Ok(
            calculate_block_page_layout(KeyType::serialized_size() + ValueType::serialized_size())?
                .max_values,
        )
    }
    /// Iterate over entries in the page
    fn iter_entries<'b>(
        &'b self,
        from_slot: usize,
        to_slot: Option<usize>,
    ) -> Result<EntryIterator<'b, 'a, KeyType, ValueType>, HashTableBlockError>;
    /// Fraction of entries that are filled
    fn fraction_slots_occupied(&self) -> Result<f32, HashTableBlockError> {
        let mut total = 0;
        let num_slots = self.num_slots()?;
        for i in 0..num_slots {
            if self.slot_occupied(i)? {
                total += 1;
            }
        }
        Ok(total as f32 / num_slots as f32)
    }
}

pub trait IHashTableBlockPageWrite<'a, KeyType: BytesSerialize, ValueType: BytesSerialize>:
    IHashTableBlockPageRead<'a, KeyType, ValueType>
{
    fn put_slot(
        &mut self,
        slot: usize,
        key: KeyType,
        value: ValueType,
    ) -> Result<(), HashTableBlockError>;
    fn remove_slot(&mut self, slot: usize) -> Result<(), HashTableBlockError>;
}

#[derive(Debug)]
pub enum HashTableBlockError {
    PageError(PageError),
    SerializeError(SerializeError),
    PageLayoutError(PageLayoutError),
    SlotNotReadable,
    SlotOccupied,
}

impl From<PageError> for HashTableBlockError {
    fn from(e: PageError) -> Self {
        HashTableBlockError::PageError(e)
    }
}

impl From<SerializeError> for HashTableBlockError {
    fn from(e: SerializeError) -> Self {
        HashTableBlockError::SerializeError(e)
    }
}

impl From<PageLayoutError> for HashTableBlockError {
    fn from(e: PageLayoutError) -> Self {
        HashTableBlockError::PageLayoutError(e)
    }
}

pub struct EntryIterator<'a, 'b, KeyType: BytesSerialize, ValueType: BytesSerialize> {
    block_page: &'a dyn IHashTableBlockPageRead<'a, KeyType, ValueType>,
    current_position: usize,
    max_position: usize,
    _lifetime: std::marker::PhantomData<&'b ()>,
}

impl<'a, 'b, KeyType: BytesSerialize, ValueType: BytesSerialize>
    EntryIterator<'a, 'b, KeyType, ValueType>
{
    fn new(
        block_page: &'a dyn IHashTableBlockPageRead<'a, KeyType, ValueType>,
        from_slot: usize,
        to_slot: Option<usize>,
    ) -> Result<Self, HashTableBlockError> {
        let max_position = match to_slot {
            Some(to_slot) => to_slot,
            None => block_page.num_slots()?,
        };
        Ok(Self {
            block_page,
            current_position: from_slot,
            max_position,
            _lifetime: PhantomData,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct EntryIteratorValue<KeyType: BytesSerialize, ValueType: BytesSerialize> {
    pub entry: Option<(KeyType, ValueType)>,
    pub occupied: bool,
}

impl<'a, 'b, KeyType: BytesSerialize, ValueType: BytesSerialize> Iterator
    for EntryIterator<'a, 'b, KeyType, ValueType>
{
    type Item = EntryIteratorValue<KeyType, ValueType>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_position >= self.max_position {
            // Stop iteration
            return None;
        }
        let occupied = self
            .block_page
            .slot_occupied(self.current_position)
            .unwrap();
        let readable = self
            .block_page
            .slot_readable(self.current_position)
            .unwrap();
        if !readable {
            self.current_position += 1;
            // Empty slot
            return Some(EntryIteratorValue {
                entry: None,
                occupied,
            });
        }
        let key = self.block_page.key_at(self.current_position).unwrap();
        let value = self.block_page.value_at(self.current_position).unwrap();
        self.current_position += 1;
        Some(EntryIteratorValue {
            entry: Some((key, value)),
            occupied,
        })
    }
}

pub struct ReadOnlyHashTableBlockPage<'a, KeyType: BytesSerialize, ValueType: BytesSerialize> {
    page: ReadOnlyPage<'a>,
    layout: PageLayout,

    _phantom: PhantomData<KeyType>,
    _phantom2: PhantomData<ValueType>,
}

impl<'a, KeyType: BytesSerialize, ValueType: BytesSerialize>
    ReadOnlyHashTableBlockPage<'a, KeyType, ValueType>
{
    pub fn new(page: ReadOnlyPage<'a>) -> Self {
        let layout =
            calculate_block_page_layout(KeyType::serialized_size() + ValueType::serialized_size())
                .unwrap(); // TODO: Handle error

        Self {
            page,
            layout,
            _phantom: PhantomData,
            _phantom2: PhantomData,
        }
    }

    fn entry_offset_size(&self) -> usize {
        KeyType::serialized_size() + ValueType::serialized_size()
    }

    fn key_address(&self, slot: usize) -> usize {
        self.layout.value_array_start + slot * self.entry_offset_size()
    }

    fn value_address(&self, slot: usize) -> usize {
        self.layout.value_array_start + slot * self.entry_offset_size() + KeyType::serialized_size()
    }

    fn read_key(&self, slot: usize) -> Result<KeyType, HashTableBlockError> {
        let readable = self.read_readable(slot)?;
        if !readable {
            return Err(HashTableBlockError::SlotNotReadable);
        }

        let key_address = self.key_address(slot);
        let key_bytes = self
            .page
            .read_data(key_address, KeyType::serialized_size())?;
        Ok(KeyType::from_bytes(key_bytes)?)
    }

    fn read_value(&self, slot: usize) -> Result<ValueType, HashTableBlockError> {
        let readable = self.read_readable(slot)?;
        if !readable {
            return Err(HashTableBlockError::SlotNotReadable);
        }

        let value_address = self.value_address(slot);
        let value_bytes = self
            .page
            .read_data(value_address, ValueType::serialized_size())?;
        Ok(ValueType::from_bytes(value_bytes)?)
    }

    fn _read_bit_block(
        &self,
        block_start: usize,
        slot: usize,
    ) -> Result<bool, HashTableBlockError> {
        let byte_address = block_start + slot / 8;
        let byte = self.page.read_data(byte_address, 1)?[0];
        let bit = (byte >> (slot % 8)) & 1;
        Ok(bit == 1)
    }

    fn read_occupied(&self, slot: usize) -> Result<bool, HashTableBlockError> {
        self._read_bit_block(self.layout.occupancy_array_start, slot)
    }

    fn read_readable(&self, slot: usize) -> Result<bool, HashTableBlockError> {
        self._read_bit_block(self.layout.readability_array_start, slot)
    }
}

impl<'a, KeyType: BytesSerialize, ValueType: BytesSerialize>
    IHashTableBlockPageRead<'a, KeyType, ValueType>
    for ReadOnlyHashTableBlockPage<'a, KeyType, ValueType>
{
    fn key_at(&self, slot: usize) -> Result<KeyType, HashTableBlockError> {
        self.read_key(slot)
    }

    fn value_at(&self, slot: usize) -> Result<ValueType, HashTableBlockError> {
        self.read_value(slot)
    }

    fn slot_occupied(&self, slot: usize) -> Result<bool, HashTableBlockError> {
        self.read_occupied(slot)
    }

    fn slot_readable(&self, slot: usize) -> Result<bool, HashTableBlockError> {
        self.read_readable(slot)
    }

    fn iter_entries<'b>(
        &'b self,
        from_slot: usize,
        to_slot: Option<usize>,
    ) -> Result<EntryIterator<'b, 'a, KeyType, ValueType>, HashTableBlockError> {
        EntryIterator::new(self, from_slot, to_slot)
    }
}

pub struct WritableHashTableBlockPage<'a, KeyType: BytesSerialize, ValueType: BytesSerialize> {
    page: WritablePage<'a>,
    layout: PageLayout,

    _phantom: PhantomData<KeyType>,
    _phantom2: PhantomData<ValueType>,
}

impl<'a, KeyType: BytesSerialize, ValueType: BytesSerialize>
    WritableHashTableBlockPage<'a, KeyType, ValueType>
{
    pub fn new(page: WritablePage<'a>) -> Self {
        let layout =
            calculate_block_page_layout(KeyType::serialized_size() + ValueType::serialized_size())
                .unwrap(); // TODO: Handle error

        Self {
            page,
            layout,
            _phantom: PhantomData,
            _phantom2: PhantomData,
        }
    }

    fn entry_offset_size(&self) -> usize {
        KeyType::serialized_size() + ValueType::serialized_size()
    }

    fn key_address(&self, slot: usize) -> usize {
        self.layout.value_array_start + slot * self.entry_offset_size()
    }

    fn value_address(&self, slot: usize) -> usize {
        self.layout.value_array_start + slot * self.entry_offset_size() + KeyType::serialized_size()
    }

    fn read_key(&self, slot: usize) -> Result<KeyType, HashTableBlockError> {
        let readable = self.read_readable(slot)?;
        if !readable {
            return Err(HashTableBlockError::SlotNotReadable);
        }

        let key_address = self.key_address(slot);
        let key_bytes = self
            .page
            .read_data(key_address, KeyType::serialized_size())?;
        Ok(KeyType::from_bytes(key_bytes)?)
    }

    fn read_value(&self, slot: usize) -> Result<ValueType, HashTableBlockError> {
        let readable = self.read_readable(slot)?;
        if !readable {
            return Err(HashTableBlockError::SlotNotReadable);
        }

        let value_address = self.value_address(slot);
        let value_bytes = self
            .page
            .read_data(value_address, ValueType::serialized_size())?;
        Ok(ValueType::from_bytes(value_bytes)?)
    }

    fn _read_bit_block(
        &self,
        block_start: usize,
        slot: usize,
    ) -> Result<bool, HashTableBlockError> {
        let byte_address = block_start + slot / 8;
        let byte = self.page.read_data(byte_address, 1)?[0];
        let bit = (byte >> (slot % 8)) & 1;
        Ok(bit == 1)
    }

    fn read_occupied(&self, slot: usize) -> Result<bool, HashTableBlockError> {
        self._read_bit_block(self.layout.occupancy_array_start, slot)
    }

    fn read_readable(&self, slot: usize) -> Result<bool, HashTableBlockError> {
        self._read_bit_block(self.layout.readability_array_start, slot)
    }

    fn write_key(&mut self, slot: usize, key: KeyType) -> Result<(), HashTableBlockError> {
        let key_address = self.key_address(slot);
        let key_bytes = key.to_bytes()?;
        Ok(self.page.write_data(key_address, &key_bytes)?)
    }

    fn write_value(&mut self, slot: usize, value: ValueType) -> Result<(), HashTableBlockError> {
        let value_address = self.value_address(slot);
        let value_bytes = value.to_bytes()?;
        Ok(self.page.write_data(value_address, &value_bytes)?)
    }

    fn write_occupied(&mut self, slot: usize, occupied: bool) -> Result<(), HashTableBlockError> {
        let byte_address = self.layout.occupancy_array_start + slot / 8;
        let mut byte = self.page.read_data(byte_address, 1)?[0];
        let bit = 1 << (slot % 8);
        if occupied {
            byte |= bit;
        } else {
            byte &= !bit;
        }
        Ok(self.page.write_data(byte_address, &[byte])?)
    }

    fn write_readable(&mut self, slot: usize, readable: bool) -> Result<(), HashTableBlockError> {
        let byte_address = self.layout.readability_array_start + slot / 8;
        let mut byte = self.page.read_data(byte_address, 1)?[0];
        let bit = 1 << (slot % 8);
        if readable {
            byte |= bit;
        } else {
            byte &= !bit;
        }
        Ok(self.page.write_data(byte_address, &[byte])?)
    }
}

impl<'a, KeyType: BytesSerialize, ValueType: BytesSerialize>
    IHashTableBlockPageRead<'a, KeyType, ValueType>
    for WritableHashTableBlockPage<'a, KeyType, ValueType>
{
    fn key_at(&self, slot: usize) -> Result<KeyType, HashTableBlockError> {
        self.read_key(slot)
    }

    fn value_at(&self, slot: usize) -> Result<ValueType, HashTableBlockError> {
        self.read_value(slot)
    }

    fn slot_occupied(&self, slot: usize) -> Result<bool, HashTableBlockError> {
        self.read_occupied(slot)
    }

    fn slot_readable(&self, slot: usize) -> Result<bool, HashTableBlockError> {
        self.read_readable(slot)
    }

    fn iter_entries<'b>(
        &'b self,
        from_slot: usize,
        to_slot: Option<usize>,
    ) -> Result<EntryIterator<'b, 'a, KeyType, ValueType>, HashTableBlockError> {
        EntryIterator::new(self, from_slot, to_slot)
    }
}

impl<'a, KeyType: BytesSerialize, ValueType: BytesSerialize>
    IHashTableBlockPageWrite<'a, KeyType, ValueType>
    for WritableHashTableBlockPage<'a, KeyType, ValueType>
{
    fn put_slot(
        &mut self,
        slot: usize,
        key: KeyType,
        value: ValueType,
    ) -> Result<(), HashTableBlockError> {
        if self.read_occupied(slot)? {
            return Err(HashTableBlockError::SlotOccupied);
        }

        self.write_key(slot, key)?;
        self.write_value(slot, value)?;
        self.write_occupied(slot, true)?;
        self.write_readable(slot, true)?;
        Ok(())
    }

    fn remove_slot(&mut self, slot: usize) -> Result<(), HashTableBlockError> {
        self.write_readable(slot, false)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::dbms::buffer::pool_manager::testing::create_testing_pool_manager;
    use crate::dbms::buffer::pool_manager::IBufferPoolManager;
    use crate::{tuple, tuple_type};

    use super::*;
    use rstest::*;

    #[rstest]
    fn test_writable_block_page_put_and_read_slot() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        // Create a block page with u32 keys and (bool, f64) values
        let mut block_page =
            WritableHashTableBlockPage::<tuple_type![u32], tuple_type![bool, f64]>::new(page);

        // Put a key-value pair in the first slot
        let key = tuple![1];
        let value = tuple![true, 1.0];
        block_page.put_slot(0, key, value).unwrap();

        // Read the key-value pair back
        let read_key = block_page.key_at(0).unwrap();
        let read_value = block_page.value_at(0).unwrap();

        assert_eq!(read_key, key);
        assert_eq!(read_value, value);
    }

    #[rstest]
    fn test_writable_block_page_write_to_used_slot() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        // Create a block page with u32 keys and (bool, f64) values
        let mut block_page =
            WritableHashTableBlockPage::<tuple_type![u32], tuple_type![bool, f64]>::new(page);

        // Put a key-value pair in the first slot
        let key1 = tuple![1];
        let value1 = tuple![true, 1.0];
        block_page.put_slot(0, key1, value1).unwrap();

        // Put a key-value pair in the first slot again
        let key2 = tuple![2];
        let value2 = tuple![false, 2.0];
        let result = block_page.put_slot(0, key2, value2);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HashTableBlockError::SlotOccupied
        ));
    }

    #[rstest]
    fn test_writable_block_page_write_to_used_removed_slot() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        // Create a block page with u32 keys and (bool, f64) values
        let mut block_page =
            WritableHashTableBlockPage::<tuple_type![u32], tuple_type![bool, f64]>::new(page);

        // Put a key-value pair in the first slot
        let key1 = tuple![1];
        let value1 = tuple![true, 1.0];
        block_page.put_slot(0, key1, value1).unwrap();

        // Remove the first slot
        block_page.remove_slot(0).unwrap();

        // Put a key-value pair in the first slot again
        let key2 = tuple![2];
        let value2 = tuple![false, 2.0];
        let result = block_page.put_slot(0, key2, value2);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HashTableBlockError::SlotOccupied
        ));
    }

    #[rstest]
    fn test_writable_block_page_remove_slot() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        // Create a block page with u32 keys and (bool, f64) values
        let mut block_page =
            WritableHashTableBlockPage::<tuple_type![u32], tuple_type![bool, f64]>::new(page);

        // Put a key-value pair in the first slot
        let key1 = tuple![1];
        let value1 = tuple![true, 1.0];
        block_page.put_slot(0, key1, value1).unwrap();

        // Remove the first slot
        block_page.remove_slot(0).unwrap();

        // Attempt to read the key-value pair back
        let key_res = block_page.key_at(0);
        let value_res = block_page.value_at(0);

        assert!(key_res.is_err());
        assert!(matches!(
            key_res.unwrap_err(),
            HashTableBlockError::SlotNotReadable
        ));

        assert!(value_res.is_err());
        assert!(matches!(
            value_res.unwrap_err(),
            HashTableBlockError::SlotNotReadable
        ));
    }

    #[rstest]
    fn test_writable_block_page_remove_slot_twice() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        // Create a block page with u32 keys and (bool, f64) values
        let mut block_page =
            WritableHashTableBlockPage::<tuple_type![u32], tuple_type![bool, f64]>::new(page);

        // Put a key-value pair in the first slot
        let key1 = tuple![1];
        let value1 = tuple![true, 1.0];
        block_page.put_slot(0, key1, value1).unwrap();

        // Remove the first slot
        block_page.remove_slot(0).unwrap();

        // Remove the first slot again
        let result = block_page.remove_slot(0);

        assert!(result.is_ok());
    }

    #[rstest]
    fn test_writable_block_page_fill_page() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        // Create a block page with u32 keys and (bool, f64) values
        let mut block_page =
            WritableHashTableBlockPage::<tuple_type![u32], tuple_type![bool, f64]>::new(page);

        // Fill the page with key-value pairs
        for i in 0..block_page.num_slots().unwrap() {
            let key = tuple![i as u32];
            let value = tuple![true, i as f64 / 3f64];
            block_page.put_slot(i, key, value).unwrap();
        }

        // Read the key-value pairs back
        for i in 0..block_page.num_slots().unwrap() {
            let key = tuple![i as u32];
            let value = tuple![true, i as f64 / 3f64];
            let read_key = block_page.key_at(i).unwrap();
            let read_value = block_page.value_at(i).unwrap();

            assert_eq!(read_key, key);
            assert_eq!(read_value, value);
        }
    }

    #[rstest]
    fn test_writable_block_page_get_key_at() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        // Create a block page with u32 keys and (bool, f64) values
        let mut block_page =
            WritableHashTableBlockPage::<tuple_type![u32], tuple_type![bool, f64]>::new(page);

        // Put a key-value pair in the first slot
        let key = tuple![1];
        let value = tuple![true, 1.0];
        block_page.put_slot(0, key, value).unwrap();

        // Read the key back
        let read_key = block_page.key_at(0).unwrap();

        assert_eq!(read_key, key);
    }

    #[rstest]
    fn test_writable_block_page_get_value_at() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        // Create a block page with u32 keys and (bool, f64) values
        let mut block_page =
            WritableHashTableBlockPage::<tuple_type![u32], tuple_type![bool, f64]>::new(page);

        // Put a key-value pair in the first slot
        let key = tuple![1];
        let value = tuple![true, 1.0];
        block_page.put_slot(0, key, value).unwrap();

        // Read the value back
        let read_value = block_page.value_at(0).unwrap();

        assert_eq!(read_value, value);
    }

    #[rstest]
    fn test_writable_block_get_slot_occupied() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        // Create a block page with u32 keys and (bool, f64) values
        let mut block_page =
            WritableHashTableBlockPage::<tuple_type![u32], tuple_type![bool, f64]>::new(page);

        // Put a key-value pair in the first slot
        let key1 = tuple![1];
        let value1 = tuple![true, 1.0];
        block_page.put_slot(0, key1, value1).unwrap();

        // Put a key-value pair in the second slot, then delete it
        let key2 = tuple![2];
        let value2 = tuple![false, 2.0];
        block_page.put_slot(1, key2, value2).unwrap();
        block_page.remove_slot(1).unwrap();

        // First slot --> occupied
        // Second slot --> occupied
        // Third slot --> unoccupied
        assert!(block_page.slot_occupied(0).unwrap());
        assert!(block_page.slot_occupied(1).unwrap());
        assert!(!block_page.slot_occupied(2).unwrap());
    }

    #[rstest]
    fn test_writable_block_get_slot_readable() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        // Create a block page with u32 keys and (bool, f64) values
        let mut block_page =
            WritableHashTableBlockPage::<tuple_type![u32], tuple_type![bool, f64]>::new(page);

        // Put a key-value pair in the first slot
        let key1 = tuple![1];
        let value1 = tuple![true, 1.0];
        block_page.put_slot(0, key1, value1).unwrap();

        // Put a key-value pair in the second slot, then delete it
        let key2 = tuple![2];
        let value2 = tuple![false, 2.0];
        block_page.put_slot(1, key2, value2).unwrap();
        block_page.remove_slot(1).unwrap();

        // First slot --> readable
        // Second slot --> unreadable
        // Third slot --> unreadable
        assert!(block_page.slot_readable(0).unwrap());
        assert!(!block_page.slot_readable(1).unwrap());
        assert!(!block_page.slot_readable(2).unwrap());
    }

    #[rstest]
    fn test_writable_block_fraction_slots_occupied() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        // Create a block page with u32 keys and (bool, f64) values
        let mut block_page =
            WritableHashTableBlockPage::<tuple_type![u32], tuple_type![bool, f64]>::new(page);

        // Put a key-value pair in the first slot
        let key1 = tuple![1];
        let value1 = tuple![true, 1.0];
        block_page.put_slot(0, key1, value1).unwrap();

        // Put a key-value pair in the second slot, then delete it
        let key2 = tuple![2];
        let value2 = tuple![false, 2.0];
        block_page.put_slot(1, key2, value2).unwrap();
        block_page.remove_slot(1).unwrap();

        assert_eq!(
            block_page.fraction_slots_occupied().unwrap(),
            2f32 / block_page.num_slots().unwrap() as f32
        );
    }

    #[rstest]
    fn test_iter_entries() {
        let pool_manager = create_testing_pool_manager(100);
        let page = pool_manager.new_page().unwrap();

        // Create a block page with u32 keys and (bool, f64) values
        let mut block_page =
            WritableHashTableBlockPage::<tuple_type![u32], tuple_type![bool, f64]>::new(page);

        // Fill the page with key-value pairs
        for i in 0..block_page.num_slots().unwrap() {
            if i % 2 == 0 {
                continue;
            }

            let key = tuple![i as u32];
            let value = tuple![true, i as f64 / 3f64];
            block_page.put_slot(i, key, value).unwrap();
        }

        // Iterate over the entries
        let mut iter = block_page.iter_entries(0, None).unwrap();
        for i in 0..block_page.num_slots().unwrap() {
            let key = tuple![i as u32];
            let value = tuple![true, i as f64 / 3f64];
            let iter_val = iter.next().unwrap();

            let iter_entry = iter_val.entry;
            let iter_occupied = iter_val.occupied;

            if i % 2 == 0 {
                assert!(iter_entry.is_none());
                assert!(!iter_occupied);
            } else {
                let (read_key, read_value) = iter_entry.unwrap();
                assert_eq!(read_key, key);
                assert_eq!(read_value, value);
                assert!(iter_occupied);
            }
        }

        // Iterate over just a few entries
        let iter = block_page.iter_entries(10, Some(14)).unwrap();
        let entries = iter.collect::<Vec<_>>();
        assert_eq!(entries.len(), 4);
        assert_eq!(
            entries,
            vec![
                EntryIteratorValue {
                    entry: None,
                    occupied: false
                },
                EntryIteratorValue {
                    entry: Some((tuple![11u32], tuple![true, 11f64 / 3f64])),
                    occupied: true
                },
                EntryIteratorValue {
                    entry: None,
                    occupied: false
                },
                EntryIteratorValue {
                    entry: Some((tuple![13u32], tuple![true, 13f64 / 3f64])),
                    occupied: true
                },
            ]
        );
    }

    #[rstest]
    fn test_threaded_block_page() {
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
                        let mut block_page_writer = WritableHashTableBlockPage::<
                            tuple_type![u32],
                            tuple_type![bool, f64],
                        >::new(page);
                        block_page_writer
                            .put_slot(10, tuple![90], tuple![false, 1.23])
                            .unwrap();
                    }
                    bpm.unpin_page(i, true).unwrap();
                }));
                let bpm = pool_manager.clone();
                write_threads.push(std::thread::spawn(move || {
                    {
                        let page = bpm.fetch_page_writable(i).unwrap();
                        let mut block_page_writer = WritableHashTableBlockPage::<
                            tuple_type![u32],
                            tuple_type![bool, f64],
                        >::new(page);
                        block_page_writer
                            .put_slot(11, tuple![80], tuple![true, 2.34])
                            .unwrap();
                        block_page_writer.remove_slot(11).unwrap();
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
                        let block_page_reader = ReadOnlyHashTableBlockPage::<
                            tuple_type![u32],
                            tuple_type![bool, f64],
                        >::new(page);

                        let slots = block_page_reader.num_slots().unwrap();
                        assert_eq!(slots, 309);
                    }
                    bpm.unpin_page(i, false).unwrap();
                }));
                let bpm = pool_manager.clone();
                read_threads.push(std::thread::spawn(move || {
                    {
                        let page = bpm.fetch_page(i).unwrap();
                        let block_page_reader = ReadOnlyHashTableBlockPage::<
                            tuple_type![u32],
                            tuple_type![bool, f64],
                        >::new(page);

                        let key_10 = block_page_reader.key_at(10).unwrap();
                        let value_10 = block_page_reader.value_at(10).unwrap();
                        let occupied_10 = block_page_reader.slot_occupied(10).unwrap();
                        let readable_10 = block_page_reader.slot_readable(10).unwrap();
                        assert_eq!(key_10, tuple![90]);
                        assert_eq!(value_10, tuple![false, 1.23]);
                        assert!(occupied_10);
                        assert!(readable_10);
                    }

                    bpm.unpin_page(i, false).unwrap();
                }));
                let bpm = pool_manager.clone();
                read_threads.push(std::thread::spawn(move || {
                    {
                        let page = bpm.fetch_page(i).unwrap();
                        let block_page_reader = ReadOnlyHashTableBlockPage::<
                            tuple_type![u32],
                            tuple_type![bool, f64],
                        >::new(page);

                        let key_11_res = block_page_reader.key_at(11);
                        let value_11_res = block_page_reader.value_at(11);
                        let occupied_11 = block_page_reader.slot_occupied(11).unwrap();
                        let readable_11 = block_page_reader.slot_readable(11).unwrap();
                        assert!(key_11_res.is_err());
                        assert!(value_11_res.is_err());
                        assert!(occupied_11);
                        assert!(!readable_11);
                    }

                    bpm.unpin_page(i, false).unwrap();
                }));
                let bpm = pool_manager.clone();
                read_threads.push(std::thread::spawn(move || {
                    {
                        let page = bpm.fetch_page(i).unwrap();
                        let block_page_reader = ReadOnlyHashTableBlockPage::<
                            tuple_type![u32],
                            tuple_type![bool, f64],
                        >::new(page);

                        let key_12_res = block_page_reader.key_at(12);
                        let value_12_res = block_page_reader.value_at(12);
                        let occupied_12 = block_page_reader.slot_occupied(12).unwrap();
                        let readable_12 = block_page_reader.slot_readable(12).unwrap();
                        assert!(key_12_res.is_err());
                        assert!(value_12_res.is_err());
                        assert!(!occupied_12);
                        assert!(!readable_12);
                    }

                    bpm.unpin_page(i, false).unwrap();
                }));
                let bpm = pool_manager.clone();
                read_threads.push(std::thread::spawn(move || {
                    {
                        let page = bpm.fetch_page(i).unwrap();
                        let block_page_reader = ReadOnlyHashTableBlockPage::<
                            tuple_type![u32],
                            tuple_type![bool, f64],
                        >::new(page);

                        assert_eq!(
                            block_page_reader.fraction_slots_occupied().unwrap(),
                            2f32 / 309f32
                        );
                    }

                    bpm.unpin_page(i, false).unwrap();
                }));
            }
        }

        for thread in read_threads {
            thread.join().unwrap();
        }
    }

    #[rstest]
    fn test_threaded_iter_entries() {
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
                        let mut block_page_writer = WritableHashTableBlockPage::<
                            tuple_type![u32],
                            tuple_type![bool, f64],
                        >::new(page);
                        block_page_writer
                            .put_slot(10, tuple![90], tuple![false, 1.23])
                            .unwrap();
                    }
                    bpm.unpin_page(i, true).unwrap();
                }));
                let bpm = pool_manager.clone();
                write_threads.push(std::thread::spawn(move || {
                    {
                        let page = bpm.fetch_page_writable(i).unwrap();
                        let mut block_page_writer = WritableHashTableBlockPage::<
                            tuple_type![u32],
                            tuple_type![bool, f64],
                        >::new(page);
                        block_page_writer
                            .put_slot(11, tuple![80], tuple![true, 2.34])
                            .unwrap();
                        block_page_writer.remove_slot(11).unwrap();
                    }
                    bpm.unpin_page(i, true).unwrap();
                }));
                let bpm = pool_manager.clone();
                write_threads.push(std::thread::spawn(move || {
                    {
                        let page = bpm.fetch_page_writable(i).unwrap();
                        let mut block_page_writer = WritableHashTableBlockPage::<
                            tuple_type![u32],
                            tuple_type![bool, f64],
                        >::new(page);
                        block_page_writer
                            .put_slot(12, tuple![70], tuple![true, 3.45])
                            .unwrap();
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
                for _ in 0..3 {
                    // Read the same page multiple times in different threads
                    let bpm = pool_manager.clone();
                    read_threads.push(std::thread::spawn(move || {
                        {
                            let page = bpm.fetch_page(i).unwrap();
                            let block_page_reader = ReadOnlyHashTableBlockPage::<
                                tuple_type![u32],
                                tuple_type![bool, f64],
                            >::new(page);

                            let iter = block_page_reader.iter_entries(10, Some(14)).unwrap();
                            let values = iter.collect::<Vec<_>>();
                            assert_eq!(
                                values,
                                vec![
                                    EntryIteratorValue {
                                        entry: Some((tuple![90], tuple![false, 1.23])),
                                        occupied: true
                                    },
                                    EntryIteratorValue {
                                        entry: None,
                                        occupied: true
                                    },
                                    EntryIteratorValue {
                                        entry: Some((tuple![70], tuple![true, 3.45])),
                                        occupied: true
                                    },
                                    EntryIteratorValue {
                                        entry: None,
                                        occupied: false
                                    },
                                ]
                            );
                        }
                        bpm.unpin_page(i, false).unwrap();
                    }));
                }
            }
        }

        for thread in read_threads {
            thread.join().unwrap();
        }
    }
}
