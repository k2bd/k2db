use crate::dbms::types::PAGE_SIZE;

#[derive(Debug)]
pub struct PageLayout {
    pub occupancy_array_start: usize,
    pub readability_array_start: usize,
    pub value_array_start: usize,
    pub max_values: usize,
}

#[allow(dead_code)]
pub fn calculate_block_page_layout(value_size: usize) -> Option<PageLayout> {
    if value_size == 0 {
        return None; // No zero-sized values allowed
    }

    let byte_size = 8;

    // Calculate the size of the bit arrays in bytes, rounded up to the nearest whole byte
    let bit_array_bytes = |num_values| (num_values + byte_size - 1) / byte_size;

    // Calculate how many values can fit into the page with the given value size
    // and the size of the bit arrays. Start with a rough estimate and then decrease it
    // until it fits into the page.
    let mut max_values = (PAGE_SIZE - 2) / (value_size + 1 / byte_size); // Subtract 2 for initial byte offsets
    while bit_array_bytes(max_values) * 2 + max_values * value_size > PAGE_SIZE {
        max_values -= 1;
    }

    // Check if at least one value can fit into the page
    if max_values == 0 {
        return None;
    }

    let occupancy_array_bytes = bit_array_bytes(max_values);
    let readability_array_bytes = bit_array_bytes(max_values);

    Some(PageLayout {
        occupancy_array_start: 0,
        readability_array_start: occupancy_array_bytes,
        value_array_start: occupancy_array_bytes + readability_array_bytes,
        max_values,
    })
}
