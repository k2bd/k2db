use crate::dbms::{
    buffer::types::{ReadOnlyPage, WritablePage},
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

pub trait IHashTableRead<KeyType: BytesSerialize, ValueType: BytesSerialize> {
    type KeyType;
    type ValueType;

    fn get_single_value(&self, key: KeyType) -> Result<Option<ValueType>, HashTableError>;
    fn get_all_values(&self, key: KeyType) -> Result<Vec<ValueType>, HashTableError>;
}
pub trait IHashTableWrite<KeyType: BytesSerialize, ValueType: BytesSerialize>:
    IHashTableRead<KeyType, ValueType>
{
    type KeyType;
    type ValueType;

    fn insert_entry(
        &mut self,
        key: KeyType,
        value: ValueType,
    ) -> Result<HashTableInsertResult, HashTableError>;
    fn delete_entry(
        &mut self,
        key: KeyType,
        value: ValueType,
    ) -> Result<HashTableDeleteResult, HashTableError>;
}

pub struct ReadOnlyLinearProbingHashTable<'a> {
    page: ReadOnlyPage<'a>,
}

pub struct WritableLinearProbingHashTable<'a> {
    page: WritablePage<'a>,
}
