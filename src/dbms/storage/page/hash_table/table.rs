use crate::dbms::{
    buffer::{
        pool_manager::IBufferPoolManager,
        types::{ReadOnlyPage, WritablePage},
    },
    storage::serialize::BytesSerialize,
};

pub enum HashTableInsertResult {
    Inserted,
    DuplicateEntry,
}
pub enum HashTableDeleteResult {
    Deleted,
    DidNotExist,
}
pub enum HashTableError {}

pub trait IHashTable<KeyType: BytesSerialize, ValueType: BytesSerialize> {
    type KeyType;
    type ValueType;

    fn get_single_value(
        &self,
        pool: &impl IBufferPoolManager,
        key: KeyType,
    ) -> Result<Option<ValueType>, HashTableError>;
    fn get_all_values(
        &self,
        pool: &impl IBufferPoolManager,
        key: KeyType,
    ) -> Result<Vec<ValueType>, HashTableError>;
    fn insert_entry(
        &mut self,
        pool: &mut impl IBufferPoolManager,
        key: KeyType,
        value: ValueType,
    ) -> Result<HashTableInsertResult, HashTableError>;
    fn delete_entry(
        &mut self,
        pool: &mut impl IBufferPoolManager,
        key: KeyType,
        value: ValueType,
    ) -> Result<HashTableDeleteResult, HashTableError>;
}

pub struct LinearProbingHashTable {}
