use crate::dbms::types::PAGE_SIZE;

#[derive(Debug)]
pub struct PageLayout {
    pub occupancy_array_start: usize,
    pub readability_array_start: usize,
    pub value_array_start: usize,
    pub max_values: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PageLayoutError {
    BadValueSize(String),
}

#[allow(dead_code)]
pub fn calculate_block_page_layout(entry_size: usize) -> Result<PageLayout, PageLayoutError> {
    if entry_size == 0 {
        return Err(PageLayoutError::BadValueSize(
            "Value size must be greater than 0".to_string(),
        ));
    }

    let byte_size = 8;

    // Calculate the size of the bit arrays in bytes, rounded up to the nearest whole byte
    let bit_array_bytes = |num_values| (num_values + byte_size - 1) / byte_size;

    // Calculate how many values can fit into the page with the given value size
    // and the size of the bit arrays. Start with a rough estimate and then decrease it
    // until it fits into the page.
    let mut max_values = (PAGE_SIZE - 2) / (entry_size + 1 / byte_size); // Subtract 2 for initial byte offsets
    while bit_array_bytes(max_values) * 2 + max_values * entry_size > PAGE_SIZE {
        max_values -= 1;
    }

    // Check if at least one value can fit into the page
    if max_values == 0 {
        return Err(PageLayoutError::BadValueSize(format!(
            "Value size {} is too large for page size {}",
            entry_size, PAGE_SIZE
        )));
    }

    let occupancy_array_bytes = bit_array_bytes(max_values);
    let readability_array_bytes = bit_array_bytes(max_values);

    Ok(PageLayout {
        occupancy_array_start: 0,
        readability_array_start: occupancy_array_bytes,
        value_array_start: occupancy_array_bytes + readability_array_bytes,
        max_values,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    #[case(32, 0, 16, 32, 127)]
    #[case(1, 0, 410, 820, 3276)]
    #[case(10, 0, 50, 100, 399)]
    #[case(256, 0, 2, 4, 15)]
    #[case(4094, 0, 1, 2, 1)]
    fn test_calculate_block_page_layout(
        #[case] entry_size: usize,
        #[case] exp_occupancy_array_start: usize,
        #[case] exp_readability_array_start: usize,
        #[case] exp_value_array_start: usize,
        #[case] exp_max_values: usize,
    ) {
        let layout = calculate_block_page_layout(entry_size).unwrap();
        assert_eq!(layout.occupancy_array_start, exp_occupancy_array_start);
        assert_eq!(layout.readability_array_start, exp_readability_array_start);
        assert_eq!(layout.value_array_start, exp_value_array_start);
        assert_eq!(layout.max_values, exp_max_values);

        let total_bytes = layout.value_array_start + layout.max_values * entry_size;
        assert!(total_bytes <= PAGE_SIZE);
    }

    #[rstest]
    #[case(4096)]
    #[case(4095)]
    #[case(10_000)]
    fn test_calculate_block_page_layout_too_large(#[case] entry_size: usize) {
        let layout = calculate_block_page_layout(entry_size);
        assert!(layout.is_err());
        assert_eq!(
            layout.unwrap_err(),
            PageLayoutError::BadValueSize(format!(
                "Value size {} is too large for page size {}",
                entry_size, PAGE_SIZE
            ))
        );
    }

    #[rstest]
    fn test_calculate_block_page_layout_size_0() {
        let layout = calculate_block_page_layout(0);
        assert!(layout.is_err());
        assert_eq!(
            layout.unwrap_err(),
            PageLayoutError::BadValueSize("Value size must be greater than 0".to_string())
        );
    }
}
