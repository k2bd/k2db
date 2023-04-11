#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
pub trait IDiskManager {
    fn write_page(&mut self, page_id: usize, page: &[u8]) -> Result<(), String>;
    fn read_page(&mut self, page_id: usize) -> Result<Vec<u8>, String>;
    fn write_log(&mut self, log: &[u8]) -> Result<(), String>;
    fn read_log(&mut self, size: usize, offset: usize) -> Result<Vec<u8>, String>;
    fn allocate_page(&mut self) -> Result<usize, String>;
    fn deallocate_page(&mut self, page_id: usize) -> Result<(), String>;
}
