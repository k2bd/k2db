use std::marker::PhantomData;

use crate::dbms::{
    buffer::types::ReadOnlyPage,
    storage::{page::PageError, serialize::BytesSerialize},
};

use super::util::{calculate_block_page_layout, PageLayout};

pub trait IHashTableBlockPageRead<KeyType: BytesSerialize, ValueType: BytesSerialize> {
    type KeyType;
    type ValueType;

    fn key_at(&self, offset: usize) -> Result<KeyType, HashTableBlockError>;
    fn value_at(&self, offset: usize) -> Result<ValueType, HashTableBlockError>;
    fn slot_occupied(&self, offset: usize) -> Result<bool, HashTableBlockError>;
    fn slot_readable(&self, offset: usize) -> Result<bool, HashTableBlockError>;
}

pub trait IHashTableBlockPageWrite<KeyType: BytesSerialize, ValueType: BytesSerialize>:
    IHashTableBlockPageRead<KeyType, ValueType>
{
    fn put_slot(
        &mut self,
        offset: usize,
        key: KeyType,
        value: ValueType,
    ) -> Result<(), HashTableBlockError>;
    fn remove_slot(&mut self, offset: usize) -> Result<(), HashTableBlockError>;
}

#[allow(dead_code)]
pub enum HashTableBlockError {
    PageError(PageError),
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

    fn key_address(&self, offset: usize) -> usize {
        self.layout.value_array_start + offset * self.entry_offset_size()
    }

    fn value_address(&self, offset: usize) -> usize {
        self.layout.value_array_start
            + offset * self.entry_offset_size()
            + KeyType::serialized_size()
    }

    fn read_key(&self, offset: usize) -> Result<KeyType, HashTableBlockError> {
        let key_address = self.key_address(offset);
        let key_bytes = self
            .page
            .read_data(key_address, KeyType::serialized_size())?;
        Ok(KeyType::from_bytes(key_bytes))
    }

    fn read_value(&self, offset: usize) -> Result<ValueType, HashTableBlockError> {
        let value_address = self.value_address(offset);
        let value_bytes = self
            .page
            .read_data(value_address, ValueType::serialized_size())?;
        Ok(ValueType::from_bytes(value_bytes))
    }

    fn _read_bit_block(
        &self,
        block_start: usize,
        offset: usize,
    ) -> Result<bool, HashTableBlockError> {
        let byte_address = block_start + offset / 8;
        let byte = self.page.read_data(byte_address, 1)?[0];
        let bit = (byte >> (offset % 8)) & 1;
        Ok(bit == 1)
    }

    fn read_occupied(&self, offset: usize) -> Result<bool, HashTableBlockError> {
        self._read_bit_block(self.layout.occupancy_array_start, offset)
    }

    fn read_readable(&self, offset: usize) -> Result<bool, HashTableBlockError> {
        self._read_bit_block(self.layout.readability_array_start, offset)
    }
}

impl<'a, KeyType: BytesSerialize, ValueType: BytesSerialize>
    IHashTableBlockPageRead<KeyType, ValueType>
    for ReadOnlyHashTableBlockPage<'a, KeyType, ValueType>
{
    type KeyType = KeyType;
    type ValueType = ValueType;

    fn key_at(&self, offset: usize) -> Result<KeyType, HashTableBlockError> {
        self.read_key(offset)
    }

    fn value_at(&self, offset: usize) -> Result<ValueType, HashTableBlockError> {
        self.read_value(offset)
    }

    fn slot_occupied(&self, offset: usize) -> Result<bool, HashTableBlockError> {
        self.read_occupied(offset)
    }

    fn slot_readable(&self, offset: usize) -> Result<bool, HashTableBlockError> {
        self.read_readable(offset)
    }
}

#[cfg(test)]
mod tests {
    use crate::tuple_type;

    use super::*;
    use rstest::*;
}
