use std::marker::PhantomData;

use crate::dbms::{
    buffer::types::{ReadOnlyPage, WritablePage},
    storage::{page::PageError, serialize::BytesSerialize},
};

use super::util::{calculate_block_page_layout, PageLayout};

pub trait IHashTableBlockPageRead<KeyType: BytesSerialize, ValueType: BytesSerialize> {
    type KeyType;
    type ValueType;

    fn key_at(&self, slot: usize) -> Result<KeyType, HashTableBlockError>;
    fn value_at(&self, slot: usize) -> Result<ValueType, HashTableBlockError>;
    fn slot_occupied(&self, slot: usize) -> Result<bool, HashTableBlockError>;
    fn slot_readable(&self, slot: usize) -> Result<bool, HashTableBlockError>;
}

pub trait IHashTableBlockPageWrite<KeyType: BytesSerialize, ValueType: BytesSerialize>:
    IHashTableBlockPageRead<KeyType, ValueType>
{
    fn put_slot(
        &mut self,
        slot: usize,
        key: KeyType,
        value: ValueType,
    ) -> Result<(), HashTableBlockError>;
    fn remove_slot(&mut self, slot: usize) -> Result<(), HashTableBlockError>;
}

#[allow(dead_code)]
pub enum HashTableBlockError {
    PageError(PageError),
    SlotNotReadable,
}

impl From<PageError> for HashTableBlockError {
    fn from(e: PageError) -> Self {
        HashTableBlockError::PageError(e)
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
    #[allow(dead_code)]
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
        Ok(KeyType::from_bytes(key_bytes))
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
        Ok(ValueType::from_bytes(value_bytes))
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
    IHashTableBlockPageRead<KeyType, ValueType>
    for ReadOnlyHashTableBlockPage<'a, KeyType, ValueType>
{
    type KeyType = KeyType;
    type ValueType = ValueType;

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
    #[allow(dead_code)]
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
        Ok(KeyType::from_bytes(key_bytes))
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
        Ok(ValueType::from_bytes(value_bytes))
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
        let key_bytes = key.to_bytes();
        Ok(self.page.write_data(key_address, &key_bytes)?)
    }

    fn write_value(&mut self, slot: usize, value: ValueType) -> Result<(), HashTableBlockError> {
        let value_address = self.value_address(slot);
        let value_bytes = value.to_bytes();
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
    IHashTableBlockPageRead<KeyType, ValueType>
    for WritableHashTableBlockPage<'a, KeyType, ValueType>
{
    type KeyType = KeyType;
    type ValueType = ValueType;

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
}

impl<'a, KeyType: BytesSerialize, ValueType: BytesSerialize>
    IHashTableBlockPageWrite<KeyType, ValueType>
    for WritableHashTableBlockPage<'a, KeyType, ValueType>
{
    fn put_slot(
        &mut self,
        slot: usize,
        key: KeyType,
        value: ValueType,
    ) -> Result<(), HashTableBlockError> {
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
    use crate::tuple_type;

    use super::*;
    use rstest::*;
}
