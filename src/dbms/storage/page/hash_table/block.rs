use crate::dbms::storage::{page::PageError, serialize::BytesSerialize};

pub trait IHashTableBlockPage<
    const KEY_SIZE: usize,
    const VALUE_SIZE: usize,
    KeyType: BytesSerialize<KEY_SIZE>,
    ValueType: BytesSerialize<VALUE_SIZE>,
>
{
    fn key_at(&self, offset: usize) -> Result<KeyType, HashTableBlockError>;
    fn value_at(&self, offset: usize) -> Result<ValueType, HashTableBlockError>;
    fn put_slot(
        &mut self,
        offset: usize,
        key: KeyType,
        value: ValueType,
    ) -> Result<(), HashTableBlockError>;
    fn remove_slot(&mut self, offset: usize) -> Result<(), HashTableBlockError>;
    fn slot_occupied(&self, offset: usize) -> Result<bool, HashTableBlockError>;
    fn slot_readable(&self, offset: usize) -> Result<bool, HashTableBlockError>;
}

#[allow(dead_code)]
pub enum HashTableBlockError {
    PageError(PageError),
}
