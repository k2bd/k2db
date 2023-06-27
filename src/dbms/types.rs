pub const PAGE_SIZE: usize = 4096;

pub type PageData = [u8; PAGE_SIZE];

pub type PageId = u32;

pub const NULL_PAGE_ID: PageId = PageId::MAX;
