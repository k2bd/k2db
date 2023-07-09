use xxhash_rust::xxh3::xxh3_64_with_seed;

pub trait HashFunction {
    fn new(seed: u64) -> Self
    where
        Self: Sized;
    fn hash(&self, key: &[u8], table_size: usize) -> usize;
}

pub struct XxHashFunction {
    seed: u64,
}

impl HashFunction for XxHashFunction {
    fn new(seed: u64) -> Self {
        Self { seed }
    }

    fn hash(&self, key: &[u8], table_size: usize) -> usize {
        let hash_val = xxh3_64_with_seed(key, self.seed);
        (hash_val % table_size as u64) as usize
    }
}

#[cfg(test)]
/// A hash function that always returns the same value, for maximum
/// collision.
pub struct ConstHashFunction {
    hash_val: u64,
}

#[cfg(test)]
impl HashFunction for ConstHashFunction {
    fn new(seed: u64) -> Self {
        Self { hash_val: seed }
    }

    fn hash(&self, _key: &[u8], _table_size: usize) -> usize {
        (self.hash_val % (usize::MAX as u64)) as usize
    }
}
