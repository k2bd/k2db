use crate::dbms::{types::PageId, storage::page::Page};


pub type HashTableHeaderError = ();

pub trait IHashTableHeaderPage {
    fn get_page_id(&self) -> Result<Option<PageId>, HashTableHeaderError>;
    fn set_page_id(&mut self, page_id: PageId) -> Result<(), HashTableHeaderError>;
    fn get_size(&self) -> Result<u32, HashTableHeaderError>;
    fn set_size(&mut self, size: u32) -> Result<(), HashTableHeaderError>;
    fn get_next_ind(&self) -> Result<Option<u32>, HashTableHeaderError>;
    fn set_next_ind(&mut self, next_ind: u32) -> Result<(), HashTableHeaderError>;
    fn get_lsn(&self) -> Result<u32, HashTableHeaderError>;
    fn set_lsn(&mut self, lsn: u32) -> Result<(), HashTableHeaderError>;
    fn get_block_page_id(&self, position: usize) -> Result<Option<PageId>, HashTableHeaderError>;
    fn set_block_page_id(&mut self, position: usize, page_id: PageId) -> Result<(), HashTableHeaderError>;
}
