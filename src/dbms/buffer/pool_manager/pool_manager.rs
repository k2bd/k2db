use std::sync::RwLock;

use crate::dbms::buffer::replacer::replacer::BufferPoolReplacer;

struct BufferPoolManager {
    replacer: RwLock<Box<dyn BufferPoolReplacer>>,
}
