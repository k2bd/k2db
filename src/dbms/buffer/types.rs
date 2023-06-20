use std::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::dbms::storage::{disk::IDiskManager, page::IPage};

use super::replacer::IBufferPoolReplacer;

pub type ReplacerGeneric = Box<dyn IBufferPoolReplacer + Send + Sync>;
pub type DiskManagerGeneric = Box<dyn IDiskManager + Send + Sync>;
pub type PageGeneric = Box<dyn IPage + Send + Sync>;

pub type ReadOnlyPage<'a> = RwLockReadGuard<'a, PageGeneric>;
pub type WritablePage<'a> = RwLockWriteGuard<'a, PageGeneric>;
